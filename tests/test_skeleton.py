from app.api import create_app


def test_app_builds():
    app = create_app()
    assert app.title == "plan-entitlement-migration"
