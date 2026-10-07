"""API 层测试：权限校验、越权审计、HTTP 端到端迁移流程。"""
import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.auth import Authenticator, Role
from app.store import Store


@pytest.fixture()
def client():
    store = Store()
    auth = Authenticator({
        "tok-admin": ("admin", Role.ADMIN),
        "tok-op": ("op", Role.OPERATOR),
        "tok-reader": ("ro", Role.READER),
    })
    app = create_app(store=store, authenticator=auth)
    with TestClient(app) as c:
        c.state_store = store
        yield c
    store.close()


ADMIN = {"X-Auth-Token": "tok-admin"}
OP = {"X-Auth-Token": "tok-op"}
READER = {"X-Auth-Token": "tok-reader"}


def _setup(client, users=3):
    client.put("/plans", headers=ADMIN, json={
        "plan_id": "basic", "version": 1, "quotas": {"seats": 5}})
    client.put("/plans", headers=ADMIN, json={
        "plan_id": "basic", "version": 2, "quotas": {"seats": 20}})
    for i in range(users):
        client.put("/entitlements", headers=ADMIN, json={
            "user_id": f"u{i}", "plan_id": "basic", "plan_version": 1,
            "usage": {"seats": 1}})


def test_end_to_end_migration(client):
    _setup(client, users=3)
    r = client.post("/migrations", headers=OP, json={
        "plan_id": "basic", "to_version": 2, "batch_size": 2})
    assert r.status_code == 201
    job_id = r.json()["job_id"]
    r = client.post(f"/migrations/{job_id}/run", headers=OP)
    assert r.json()["status"] == "completed"
    assert r.json()["processed"] == 3
    for i in range(3):
        rec = client.get(f"/entitlements/u{i}", headers=READER).json()
        assert rec["plan_version"] == 2
        assert rec["quotas"] == {"seats": 20}


def test_permission_denied_and_audited(client):
    _setup(client)
    # reader 无权发起迁移
    r = client.post("/migrations", headers=READER,
                    json={"plan_id": "basic", "to_version": 2})
    assert r.status_code == 403
    # operator 无权回退
    r2 = client.post("/migrations", headers=OP,
                     json={"plan_id": "basic", "to_version": 2})
    job_id = r2.json()["job_id"]
    client.post(f"/migrations/{job_id}/run", headers=OP)
    r3 = client.post(f"/migrations/{job_id}/rollback", headers=OP)
    assert r3.status_code == 403
    # 匿名请求被拒绝
    r4 = client.get("/entitlements/u0")
    assert r4.status_code == 403
    # 越权行为全部留下审计记录
    audits = client.get("/audit", headers=ADMIN).json()
    denied = [a for a in audits if a["action"] == "permission_denied"]
    assert len(denied) == 3
    actors = {a["actor"] for a in denied}
    assert actors == {"ro", "op", "<anonymous>"}
    assert any(a["detail"]["action"] == "rollback" for a in denied)


def test_rollback_via_api(client):
    _setup(client, users=2)
    job_id = client.post("/migrations", headers=OP,
                         json={"plan_id": "basic", "to_version": 2}).json()["job_id"]
    client.post(f"/migrations/{job_id}/run", headers=OP)
    r = client.post(f"/migrations/{job_id}/rollback", headers=ADMIN)
    assert r.json()["status"] == "rolled_back"
    rec = client.get("/entitlements/u0", headers=READER).json()
    assert rec["plan_version"] == 1
    assert rec["quotas"] == {"seats": 5}


def test_manage_plans_requires_admin(client):
    r = client.put("/plans", headers=OP, json={
        "plan_id": "x", "version": 1, "quotas": {}})
    assert r.status_code == 403


def test_migration_unknown_plan_rejected(client):
    r = client.post("/migrations", headers=OP,
                    json={"plan_id": "nope", "to_version": 9})
    assert r.status_code == 400
