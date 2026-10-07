"""HTTP API 层：计划管理、额度读写、迁移任务、审计查询。

所有写操作均要求 X-Auth-Token 头；越权访问返回 403 并写入审计记录。
"""
from __future__ import annotations

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

from .auth import Authenticator, PermissionDenied, Role
from .migration import MigrationEngine, MigrationError
from .models import AuditAction, EntitlementRecord, PlanDefinition
from .store import Store


class PlanIn(BaseModel):
    plan_id: str
    version: int
    quotas: dict[str, int]
    description: str = ""


class EntitlementIn(BaseModel):
    user_id: str
    plan_id: str
    plan_version: int
    usage: dict[str, int] = Field(default_factory=dict)


class MigrationIn(BaseModel):
    plan_id: str
    to_version: int
    batch_size: int = 50
    rate_limit_per_sec: float = 0.0
    batch_timeout_sec: float = 30.0


def create_app(store: Store | None = None,
               authenticator: Authenticator | None = None) -> FastAPI:
    app = FastAPI(title="plan-entitlement-migration")
    app.state.store = store or Store()
    app.state.auth = authenticator or Authenticator()
    app.state.engine = MigrationEngine(app.state.store)

    def get_store() -> Store:
        return app.state.store

    def get_engine() -> MigrationEngine:
        return app.state.engine

    def authorize(request: Request, action: str,
                  x_auth_token: str | None) -> str:
        """校验权限；越权时写审计并抛 403。"""
        try:
            actor, _ = app.state.auth.authorize(x_auth_token or "", action)
            return actor
        except PermissionDenied as e:
            app.state.store.audit(AuditAction.PERMISSION_DENIED, e.actor, None,
                                  {"action": action, "path": str(request.url.path)})
            raise HTTPException(status_code=403, detail=str(e)) from e

    # ---- 计划定义 ----

    @app.put("/plans")
    def put_plan(body: PlanIn, request: Request,
                 x_auth_token: str | None = Header(default=None)):
        authorize(request, "manage_plans", x_auth_token)
        plan = PlanDefinition(**body.model_dump())
        app.state.store.put_plan(plan)
        return {"ok": True}

    @app.get("/plans/{plan_id}/{version}")
    def get_plan(plan_id: str, version: int, request: Request,
                 x_auth_token: str | None = Header(default=None)):
        authorize(request, "read", x_auth_token)
        plan = app.state.store.get_plan(plan_id, version)
        if plan is None:
            raise HTTPException(status_code=404, detail="plan not found")
        return {"plan_id": plan.plan_id, "version": plan.version,
                "quotas": plan.quotas, "description": plan.description}

    # ---- 用户额度 ----

    @app.put("/entitlements")
    def put_entitlement(body: EntitlementIn, request: Request,
                        x_auth_token: str | None = Header(default=None)):
        authorize(request, "manage_plans", x_auth_token)
        store = app.state.store
        plan = store.get_plan(body.plan_id, body.plan_version)
        if plan is None:
            raise HTTPException(status_code=404, detail="plan version not found")
        rec = EntitlementRecord(user_id=body.user_id, plan_id=body.plan_id,
                                plan_version=body.plan_version,
                                quotas=dict(plan.quotas), usage=dict(body.usage))
        store.put_entitlement(rec)
        return {"ok": True}

    @app.get("/entitlements/{user_id}")
    def get_entitlement(user_id: str, request: Request,
                        x_auth_token: str | None = Header(default=None)):
        authorize(request, "read", x_auth_token)
        rec = app.state.store.get_entitlement(user_id)
        if rec is None:
            raise HTTPException(status_code=404, detail="entitlement not found")
        return {"user_id": rec.user_id, "plan_id": rec.plan_id,
                "plan_version": rec.plan_version, "quotas": rec.quotas,
                "usage": rec.usage, "record_version": rec.record_version}

    # ---- 迁移任务 ----

    @app.post("/migrations", status_code=201)
    def create_migration(body: MigrationIn, request: Request,
                         x_auth_token: str | None = Header(default=None)):
        actor = authorize(request, "migrate", x_auth_token)
        try:
            job = app.state.engine.create_job(
                body.plan_id, body.to_version, actor,
                batch_size=body.batch_size,
                rate_limit_per_sec=body.rate_limit_per_sec,
                batch_timeout_sec=body.batch_timeout_sec)
        except MigrationError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        return _job_view(app.state.store, job.job_id)

    @app.post("/migrations/{job_id}/run")
    def run_migration(job_id: str, request: Request,
                      x_auth_token: str | None = Header(default=None)):
        actor = authorize(request, "migrate", x_auth_token)
        try:
            app.state.engine.run(job_id, actor)
        except MigrationError as e:
            raise HTTPException(status_code=409, detail=str(e)) from e
        return _job_view(app.state.store, job_id)

    @app.post("/migrations/{job_id}/cancel")
    def cancel_migration(job_id: str, request: Request,
                         x_auth_token: str | None = Header(default=None)):
        actor = authorize(request, "cancel", x_auth_token)
        try:
            app.state.engine.cancel(job_id, actor)
        except MigrationError as e:
            raise HTTPException(status_code=409, detail=str(e)) from e
        return _job_view(app.state.store, job_id)

    @app.post("/migrations/{job_id}/rollback")
    def rollback_migration(job_id: str, request: Request,
                           x_auth_token: str | None = Header(default=None)):
        actor = authorize(request, "rollback", x_auth_token)
        try:
            app.state.engine.rollback(job_id, actor)
        except MigrationError as e:
            raise HTTPException(status_code=409, detail=str(e)) from e
        return _job_view(app.state.store, job_id)

    @app.get("/migrations/{job_id}")
    def get_migration(job_id: str, request: Request,
                      x_auth_token: str | None = Header(default=None)):
        authorize(request, "read", x_auth_token)
        return _job_view(app.state.store, job_id)

    # ---- 审计 ----

    @app.get("/audit")
    def list_audit(request: Request, job_id: str | None = None,
                   x_auth_token: str | None = Header(default=None)):
        authorize(request, "read", x_auth_token)
        entries = app.state.store.list_audit(job_id)
        return [{"audit_id": e.audit_id, "action": e.action.value, "actor": e.actor,
                 "job_id": e.job_id, "detail": e.detail, "at": e.at} for e in entries]

    return app


def _job_view(store: Store, job_id: str) -> dict:
    job = store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return {"job_id": job.job_id, "plan_id": job.plan_id,
            "from_version": job.from_version, "to_version": job.to_version,
            "status": job.status.value, "processed": job.checkpoint.processed,
            "last_user_id": job.checkpoint.last_user_id, "error": job.error,
            "batch_size": job.batch_size,
            "rate_limit_per_sec": job.rate_limit_per_sec,
            "batch_timeout_sec": job.batch_timeout_sec}
