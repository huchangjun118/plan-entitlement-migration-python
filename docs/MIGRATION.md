# 订阅计划在线迁移

本服务将「计划定义」与「用户额度」解耦：计划以**版本**形式注册，每个用户的
计划绑定与额度快照作为**一条原子记录**存储，支持在不停服的情况下把用户从
旧计划版本在线迁移到新版本。

## 核心概念

### 计划版本（PlanVersion）

- 每个计划版本包含 `entitlements`（字段名 → 默认额度）与 `migration_map`
  （新字段 → `(旧字段, 折算比例)`）。
- 旧版本数据在新版本下的解释规则由 `transform_entitlement` 决定：
  1. `migration_map` 命中的字段按比例折算（向下取整、不为负）；
  2. 新旧版本同名且未显式映射的字段保留原值；
  3. 仅存在于新版本的字段取新版本默认值；
  4. 新版本已删除的字段被丢弃。
- 用户的 `plan_id / plan_version / entitlements` 永远作为整体读写，
  **同一用户不会同时命中两套规则**。

### 迁移（Migration）

一次迁移把某个计划下所有仍停留在 `from_version` 的用户搬到 `to_version`。

状态机：

```
PENDING -> RUNNING -> COMPLETED
                 \-> CANCELLING -> CANCELLED
                 \-> FAILED
COMPLETED/FAILED/CANCELLED -> ROLLING_BACK -> ROLLED_BACK
```

## 迁移语义

### 断点续跑

- 每个用户的迁移在**单个事务**内完成：条件更新用户行（仅当仍处于
  `from_version`）、写入 `migration_records`（迁移前/后快照）、推进
  `checkpoint`，三者同生共死，**不存在部分写入**。
- 任意时刻中断（进程崩溃、异常、取消）后重新执行 `run`，都会从
  `migrations.checkpoint` 继续扫描，已提交的用户不会重复应用
  （条件更新 + 记录唯一主键），未提交的用户不会被跳过。

### 并发读写

- 读取是单行快照读，任一时刻只会看到自洽的单一版本，不会出现字段级混合。
- 正常写（`consume`）采用「读-改-事务内条件写」，与迁移的条件更新互相
  串行化；版本在期间变化时写方收到冲突错误而不是写出混合数据。
- 迁移按用户小事务推进，不持有长事务，慢批次不会阻塞在线请求。

### 回退

- `rollback` 依据 `migration_records` 中保存的迁移前快照**逆序**逐用户
  恢复，每条恢复也是独立事务。
- 仅允许对 `COMPLETED / FAILED / CANCELLED` 状态的迁移回退；运行中的迁移
  必须先 `cancel`。回退期间迁移处于 `ROLLING_BACK`，与「同一计划已有活动
  迁移」互斥检查共同保证：回退与新迁移并发时结果确定（要么回退完成后
  再迁，要么新迁移被拒绝），且每一步都有审计记录。

### 取消与超时

- `cancel` 是协作式的：在**批次边界**生效，当前批次已提交的数据保持
  完整，不会停在中间状态。
- 批次耗时超过 `batch_timeout_sec` 时迁移进入 `FAILED`（已提交批次保持
  原子性），可调整配置后 `run` 续跑。

## 配置项（MigrationConfig）

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `batch_size` | 50 | 每批扫描/迁移的用户数，必须 > 0 |
| `rate_limit_per_sec` | 0（不限速） | 迁移吞吐上限（用户/秒）。批次结束后按 `已迁移数/速率` 与实耗的差值休眠，避免慢批次拖垮在线请求 |
| `batch_timeout_sec` | 30 | 单批次耗时上限，超时迁移置为 `FAILED`（可续跑） |

## 权限与审计

- 基于角色的访问控制（请求头 `X-Actor` / `X-Role`）：

| 角色 | 权限 |
| --- | --- |
| `admin` | 全部操作 |
| `migration_operator` | 启动/取消/回退/续跑迁移，读取额度 |
| `reader` | 只读 |

- 所有迁移操作与计划写操作都会写入 `audit_log`；**越权请求被拒绝
  （403）的同时也会留下含操作者与角色的审计记录**，可通过
  `GET /audit` 追溯。

## API 一览

| 方法/路径 | 说明 |
| --- | --- |
| `POST /plans/{plan_id}/versions` | 注册计划版本（admin） |
| `GET /plans/{plan_id}/versions` | 列出计划版本 |
| `POST /users/{user_id}/assign` | 分配计划（admin） |
| `GET /users/{user_id}/entitlement` | 读取自洽的额度快照 |
| `POST /users/{user_id}/consume` | 原子扣减额度 |
| `POST /migrations` | 创建迁移（operator+） |
| `POST /migrations/{id}/run` | 执行/续跑迁移 |
| `POST /migrations/{id}/cancel` | 批次边界取消 |
| `POST /migrations/{id}/rollback` | 回退到迁移前状态 |
| `GET /migrations/{id}` | 查询迁移状态与断点 |
| `GET /audit` | 查询审计记录 |

## 运行测试

```bash
uv run pytest -q
```

覆盖：版本转换语义、中断恢复（崩溃后续跑不重复不跳过）、并发读写一致性、
回退（含部分迁移回退、回退与新迁移互斥）、取消与超时、越权与审计。
