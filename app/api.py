"""FastAPI surface for plans, entitlements and migrations."""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from .audit import AuditLog
from .auth import PermissionDeniedError, Role, require_permission
from .db import Database
from .entitlements import (
    EntitlementNotFoundError,
    EntitlementService,
    InsufficientQuotaError,
    VersionConflictError,
)
from .migrations import (
    MigrationEngine,
    MigrationNotFoundError,
    MigrationStateError,
)
from .models import MigrationConfig
from .plans import PlanNotFoundError, PlanRepository


# ----------------------------------------------------------------------
# request/response schemas
# ----------------------------------------------------------------------
class PlanVersionIn(BaseModel):
    version: int
    entitlements: dict[str, int]
    migration_map: dict[str, tuple[str, float]] = Field(default_factory=dict)


class AssignIn(BaseModel):
    plan_id: str
    plan_version: int
    entitlements: dict[str, int]


class ConsumeIn(BaseModel):
    field: str
    amount: int = 1


class MigrationIn(BaseModel):
    plan_id: str
    from_version: int
    to_version: int
    batch_size: int = 50
    rate_limit_per_sec: float = 0.0
    batch_timeout_sec: float = 30.0


# ----------------------------------------------------------------------
# auth dependency
# ----------------------------------------------------------------------
class Actor(BaseModel):
    name: str
    role: Role


def get_actor(
    x_actor: Annotated[str | None, Header()] = None,
    x_role: Annotated[str | None, Header()] = None,
) -> Actor:
    if not x_actor or not x_role:
        raise HTTPException(status_code=401, detail="missing X-Actor / X-Role")
    try:
        role = Role(x_role)
    except ValueError:
        raise HTTPException(status_code=401, detail=f"unknown role {x_role!r}")
    return Actor(name=x_actor, role=role)


def create_app(db: Database | None = None) -> FastAPI:
    db = db or Database()
    plans = PlanRepository(db)
    entitlements = EntitlementService(db)
    audit = AuditLog(db)
    engine = MigrationEngine(db, plans, audit)

    app = FastAPI(title="plan-entitlement-migration")
    app.state.db = db
    app.state.plans = plans
    app.state.entitlements = entitlements
    app.state.audit = audit
    app.state.engine = engine

    def guarded(actor: Actor, action: str, resource: str) -> None:
        """Permission check that always leaves an audit trail."""
        try:
            require_permission(actor.role, action)
        except PermissionDeniedError as exc:
            audit.record(actor.name, action, resource, False,
                         {"role": actor.role.value})
            raise HTTPException(status_code=403, detail=str(exc))
        audit.record(actor.name, action, resource, True)

    # ---------------- plans ----------------
    @app.post("/plans/{plan_id}/versions", status_code=201)
    def create_plan_version(plan_id: str, body: PlanVersionIn,
                            actor: Actor = Depends(get_actor)):
        guarded(actor, "plan.write", f"plan:{plan_id}")
        try:
            plan = plans.create_version(plan_id, body.version, body.entitlements,
                                        body.migration_map)
        except Exception as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        return {"plan_id": plan.plan_id, "version": plan.version,
                "entitlements": plan.entitlements}

    @app.get("/plans/{plan_id}/versions")
    def list_plan_versions(plan_id: str, actor: Actor = Depends(get_actor)):
        guarded(actor, "entitlement.read", f"plan:{plan_id}")
        return [{"version": p.version, "entitlements": p.entitlements}
                for p in plans.list_versions(plan_id)]

    # ---------------- entitlements ----------------
    @app.get("/users/{user_id}/entitlement")
    def get_entitlement(user_id: str, actor: Actor = Depends(get_actor)):
        guarded(actor, "entitlement.read", f"user:{user_id}")
        try:
            snap = entitlements.get(user_id)
        except EntitlementNotFoundError:
            raise HTTPException(status_code=404, detail="user not found")
        return {"user_id": snap.user_id, "plan_id": snap.plan_id,
                "plan_version": snap.plan_version,
                "entitlements": snap.entitlements}

    @app.post("/users/{user_id}/assign", status_code=201)
    def assign(user_id: str, body: AssignIn, actor: Actor = Depends(get_actor)):
        guarded(actor, "plan.write", f"user:{user_id}")
        try:
            plans.get(body.plan_id, body.plan_version)
        except PlanNotFoundError:
            raise HTTPException(status_code=404, detail="plan version not found")
        snap = entitlements.assign(user_id, body.plan_id, body.plan_version,
                                   body.entitlements)
        return {"user_id": snap.user_id, "plan_version": snap.plan_version}

    @app.post("/users/{user_id}/consume")
    def consume(user_id: str, body: ConsumeIn,
                actor: Actor = Depends(get_actor)):
        guarded(actor, "entitlement.read", f"user:{user_id}")
        try:
            snap = entitlements.consume(user_id, body.field, body.amount)
        except EntitlementNotFoundError:
            raise HTTPException(status_code=404, detail="user/field not found")
        except InsufficientQuotaError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        except VersionConflictError as exc:
            raise HTTPException(status_code=409, detail=f"conflict: {exc}")
        return {"user_id": snap.user_id, "plan_version": snap.plan_version,
                "entitlements": snap.entitlements}

    # ---------------- migrations ----------------
    @app.post("/migrations", status_code=201)
    def start_migration(body: MigrationIn, actor: Actor = Depends(get_actor)):
        guarded(actor, "migration.start", f"plan:{body.plan_id}")
        config = MigrationConfig(
            batch_size=body.batch_size,
            rate_limit_per_sec=body.rate_limit_per_sec,
            batch_timeout_sec=body.batch_timeout_sec,
        )
        try:
            m = engine.start(body.plan_id, body.from_version, body.to_version,
                             config, actor.name)
        except PlanNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except MigrationStateError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        return _migration_view(m)

    @app.get("/migrations/{migration_id}")
    def get_migration(migration_id: str, actor: Actor = Depends(get_actor)):
        guarded(actor, "entitlement.read", f"migration:{migration_id}")
        try:
            return _migration_view(engine.get(migration_id))
        except MigrationNotFoundError:
            raise HTTPException(status_code=404, detail="migration not found")

    @app.post("/migrations/{migration_id}/run")
    def run_migration(migration_id: str, actor: Actor = Depends(get_actor)):
        guarded(actor, "migration.resume", f"migration:{migration_id}")
        try:
            return _migration_view(engine.run(migration_id))
        except MigrationNotFoundError:
            raise HTTPException(status_code=404, detail="migration not found")
        except MigrationStateError as exc:
            raise HTTPException(status_code=409, detail=str(exc))

    @app.post("/migrations/{migration_id}/cancel")
    def cancel_migration(migration_id: str, actor: Actor = Depends(get_actor)):
        guarded(actor, "migration.cancel", f"migration:{migration_id}")
        try:
            return _migration_view(engine.cancel(migration_id, actor.name))
        except MigrationNotFoundError:
            raise HTTPException(status_code=404, detail="migration not found")
        except MigrationStateError as exc:
            raise HTTPException(status_code=409, detail=str(exc))

    @app.post("/migrations/{migration_id}/rollback")
    def rollback_migration(migration_id: str,
                           actor: Actor = Depends(get_actor)):
        guarded(actor, "migration.rollback", f"migration:{migration_id}")
        try:
            return _migration_view(engine.rollback(migration_id, actor.name))
        except MigrationNotFoundError:
            raise HTTPException(status_code=404, detail="migration not found")
        except MigrationStateError as exc:
            raise HTTPException(status_code=409, detail=str(exc))

    # ---------------- audit ----------------
    @app.get("/audit")
    def list_audit(actor: Actor = Depends(get_actor)):
        guarded(actor, "entitlement.read", "audit")
        return [{"actor": e.actor, "action": e.action, "resource": e.resource,
                 "allowed": e.allowed, "detail": e.detail}
                for e in audit.list()]

    return app


def _migration_view(m) -> dict:
    return {
        "migration_id": m.migration_id,
        "plan_id": m.plan_id,
        "from_version": m.from_version,
        "to_version": m.to_version,
        "status": m.status.value,
        "checkpoint": m.checkpoint,
        "created_by": m.created_by,
        "error": m.error,
    }
