# 订阅计划在线迁移

本文档说明计划/额度迁移的语义、配置项与运维操作。

## 核心概念

### 计划版本化

- 计划定义（`PlanDefinition`）以 `(plan_id, version)` 为键，**版本不可变**。
- 用户额度记录（`EntitlementRecord`）把 `plan_version` 与生效的 `quotas` 快照
  作为**一个整体**存储。读取永远返回自洽的单个版本——旧版本数据按旧版本
  规则解释，新版本数据按新版本规则解释，**不存在字段级混合**。
- 迁移时整行原子替换（`record_version` 递增），同一用户不会同时命中两套规则。

### 迁移变换

单用户迁移是纯函数：新记录的 `quotas` 整体替换为目标版本计划定义；
`usage` 仅保留新计划中仍存在的资源项，其余丢弃。

## 迁移语义

### 分批与原子提交

- 迁移按 `batch_size` 分批扫描（按 `user_id` 升序、游标推进）。
- **每个批次是一个原子事务**：批内全部用户的新记录 + checkpoint 一起落库。
  中断（崩溃/取消/超时）只会发生在事务边界，**不会留下部分写入**。

### 断点续跑

- checkpoint = `(last_user_id, processed)`，随每个批次事务提交。
- 任意时刻中断后重新调用 `run`，从最近一次已提交的 checkpoint 继续：
  已提交的用户因游标已越过而**不会重复应用**；未提交的用户**不会被跳过**。
- 对已完成/已回退的任务重复调用 `run` 是幂等空操作。
- 同一任务的并发 `run` 会被拒绝（单任务执行锁），保证批次提交序列唯一。

### 取消与超时

- `cancel` 置取消标记；执行循环在**批次边界**检查并转为 `PAUSED`，
  数据停留在上一个已提交的 checkpoint，可从断点恢复。
- 单批次处理超过 `batch_timeout_sec` 时，**放弃该批次（不提交）**并转为
  `PAUSED`（`error=batch_timeout`），调整参数后可续跑。

### 回退

- 每个批次提交前，先把用户的**迁移前完整记录**写入快照表。
- `rollback` 在单个事务中把快照整体恢复，并置任务为 `ROLLED_BACK`（幂等）。
- 运行中（`RUNNING`）的任务拒绝回退，须先取消——回退与新迁移并发时
  结果确定：要么回退先完成（新迁移从回退后的状态重新扫描），
  要么新迁移在运行中使回退被拒绝。
- 全过程（开始/每批提交/暂停/恢复/完成/回退）均写审计日志。

## 配置项

创建迁移任务（`POST /migrations`）时可配置：

| 参数 | 默认 | 说明 |
|---|---|---|
| `batch_size` | 50 | 每批处理的用户数；调小可降低单批对在线请求的影响 |
| `rate_limit_per_sec` | 0（不限速） | 单批内每秒最多处理的用户数，慢批次不拖垮在线请求 |
| `batch_timeout_sec` | 30 | 单批次处理时限，超时则放弃该批次并暂停（不产生部分写入） |

## 权限与审计

- 基于令牌 + 角色：`reader`（只读）、`operator`（迁移/取消/恢复）、
  `admin`（回退、计划与额度管理）。
- 所有请求需携带 `X-Auth-Token` 头；越权操作返回 403 并写入
  `permission_denied` 审计记录（含 actor、动作、路径）。
- 审计查询：`GET /audit` 或 `GET /audit?job_id=...`。

## API 一览

| 方法 | 路径 | 权限 | 说明 |
|---|---|---|---|
| PUT | `/plans` | admin | 写入某版本计划定义 |
| GET | `/plans/{plan_id}/{version}` | reader+ | 查询计划定义 |
| PUT | `/entitlements` | admin | 初始化用户额度 |
| GET | `/entitlements/{user_id}` | reader+ | 查询用户额度 |
| POST | `/migrations` | operator+ | 创建迁移任务 |
| POST | `/migrations/{job_id}/run` | operator+ | 执行/断点续跑 |
| POST | `/migrations/{job_id}/cancel` | operator+ | 取消（批次边界生效） |
| POST | `/migrations/{job_id}/rollback` | admin | 回退到迁移前状态 |
| GET | `/migrations/{job_id}` | reader+ | 查询任务状态与进度 |
| GET | `/audit` | reader+ | 查询审计日志 |

## 运行测试

```bash
python -m pytest tests/ -q
```
