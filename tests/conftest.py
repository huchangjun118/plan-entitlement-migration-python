import pytest

from app.audit import AuditLog
from app.db import Database
from app.entitlements import EntitlementService
from app.migrations import MigrationEngine
from app.models import MigrationConfig
from app.plans import PlanRepository


@pytest.fixture()
def db():
    database = Database(":memory:")
    yield database
    database.close()


@pytest.fixture()
def plans(db):
    return PlanRepository(db)


@pytest.fixture()
def entitlements(db):
    return EntitlementService(db)


@pytest.fixture()
def audit(db):
    return AuditLog(db)


@pytest.fixture()
def engine(db, plans, audit):
    return MigrationEngine(db, plans, audit)


@pytest.fixture()
def two_version_plan(plans):
    """basic plan v1 -> v2: rename api_calls->requests at 2x, drop storage,
    add new field seats with default."""
    plans.create_version(
        "basic", 1,
        entitlements={"api_calls": 1000, "storage_gb": 10},
        migration_map={"requests": ("api_calls", 2.0)},
    )
    plans.create_version(
        "basic", 2,
        entitlements={"requests": 5000, "seats": 3},
    )
    return "basic"


def make_users(entitlements, plan_id, version, n, quota):
    for i in range(n):
        entitlements.assign(f"user-{i:03d}", plan_id, version, dict(quota))
