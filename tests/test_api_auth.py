"""API-level permission checks and audit trail."""

import pytest
from fastapi.testclient import TestClient

from app.api import create_app

ADMIN = {"X-Actor": "alice", "X-Role": "admin"}
OP = {"X-Actor": "bob", "X-Role": "migration_operator"}
READER = {"X-Actor": "carol", "X-Role": "reader"}


@pytest.fixture()
def client():
    app = create_app()
    with TestClient(app) as c:
        # seed plan with two versions and one user
        c.post("/plans/basic/versions", headers=ADMIN, json={
            "version": 1,
            "entitlements": {"api_calls": 100},
            "migration_map": {"requests": ["api_calls", 2.0]},
        })
        c.post("/plans/basic/versions", headers=ADMIN, json={
            "version": 2,
            "entitlements": {"requests": 1000, "seats": 1},
        })
        c.post("/users/u1/assign", headers=ADMIN, json={
            "plan_id": "basic", "plan_version": 1,
            "entitlements": {"api_calls": 50},
        })
        yield c


def test_reader_cannot_start_migration_and_denial_is_audited(client):
    r = client.post("/migrations", headers=READER, json={
        "plan_id": "basic", "from_version": 1, "to_version": 2,
    })
    assert r.status_code == 403

    audit = client.get("/audit", headers=ADMIN).json()
    denials = [e for e in audit
               if e["action"] == "migration.start" and not e["allowed"]]
    assert len(denials) == 1
    assert denials[0]["actor"] == "carol"
    assert denials[0]["detail"]["role"] == "reader"


def test_operator_can_run_full_lifecycle(client):
    r = client.post("/migrations", headers=OP, json={
        "plan_id": "basic", "from_version": 1, "to_version": 2,
        "batch_size": 10,
    })
    assert r.status_code == 201
    mid = r.json()["migration_id"]

    r = client.post(f"/migrations/{mid}/run", headers=OP)
    assert r.json()["status"] == "completed"

    snap = client.get("/users/u1/entitlement", headers=READER).json()
    assert snap["plan_version"] == 2
    assert snap["entitlements"] == {"requests": 100, "seats": 1}

    r = client.post(f"/migrations/{mid}/rollback", headers=OP)
    assert r.json()["status"] == "rolled_back"
    snap = client.get("/users/u1/entitlement", headers=READER).json()
    assert snap["plan_version"] == 1
    assert snap["entitlements"] == {"api_calls": 50}

    # every step left an audit record
    audit = client.get("/audit", headers=ADMIN).json()
    actions = {e["action"] for e in audit if e["allowed"]}
    assert {"migration.start", "migration.resume",
            "migration.rollback"} <= actions


def test_reader_cannot_write_plan(client):
    r = client.post("/plans/x/versions", headers=READER, json={
        "version": 1, "entitlements": {"a": 1},
    })
    assert r.status_code == 403


def test_missing_identity_rejected(client):
    r = client.get("/users/u1/entitlement")
    assert r.status_code == 401


def test_unknown_migration_404(client):
    r = client.get("/migrations/nope", headers=ADMIN)
    assert r.status_code == 404
