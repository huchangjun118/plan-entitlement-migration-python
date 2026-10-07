"""Plan version interpretation: old data must read correctly under new rules."""

from app.models import UserEntitlement
from app.plans import transform_entitlement


def test_transform_maps_scales_and_defaults(two_version_plan, plans):
    v1 = plans.get("basic", 1)
    v2 = plans.get("basic", 2)
    src = UserEntitlement("u1", "basic", 1, {"api_calls": 100, "storage_gb": 5})

    out = transform_entitlement(src, v1, v2)

    assert out.plan_version == 2
    # mapped field carried over with ratio
    assert out.entitlements["requests"] == 200
    # new field gets the new plan default
    assert out.entitlements["seats"] == 3
    # removed field is dropped
    assert "storage_gb" not in out.entitlements


def test_transform_keeps_unmapped_common_fields(plans):
    plans.create_version("p", 1, entitlements={"a": 10, "b": 20})
    plans.create_version("p", 2, entitlements={"a": 99, "b": 99})
    src = UserEntitlement("u1", "p", 1, {"a": 7, "b": 8})

    out = transform_entitlement(src, plans.get("p", 1), plans.get("p", 2))

    # same name in both versions and no explicit mapping: value preserved
    assert out.entitlements == {"a": 7, "b": 8}


def test_transform_floors_negative_ratio_to_zero(plans):
    plans.create_version("q", 1, entitlements={"x": 10},
                         migration_map={"x": ("x", -1.0)})
    plans.create_version("q", 2, entitlements={"x": 10})
    src = UserEntitlement("u1", "q", 1, {"x": 5})

    out = transform_entitlement(src, plans.get("q", 1), plans.get("q", 2))

    assert out.entitlements["x"] == 0
