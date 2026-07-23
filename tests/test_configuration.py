import pytest
from keboola.component.exceptions import UserException

from configuration import Configuration, DateRangeMode, LoadType


def _base_params(**overrides):
    params = {
        "#consumer_key": "ck",
        "#consumer_secret": "cs",
        "#access_token": "at",
        "#access_token_secret": "ats",
        "account_ids": ["18ce54d4x5t"],
        "object": "campaigns",
    }
    params.update(overrides)
    return params


def test_valid_config_parses():
    cfg = Configuration(**_base_params())
    assert cfg.consumer_key == "ck"
    assert cfg.account_ids == ["18ce54d4x5t"]
    assert cfg.load_type == LoadType.incremental_load
    assert cfg.incremental is True


def test_full_load_not_incremental():
    cfg = Configuration(**_base_params(load_type="full_load"))
    assert cfg.incremental is False


def test_missing_secret_raises_userexception():
    params = _base_params()
    del params["#access_token"]
    with pytest.raises(UserException) as exc:
        Configuration(**params)
    assert "access_token" in str(exc.value)


def test_analytics_enabled_without_entities_raises():
    with pytest.raises(UserException) as exc:
        Configuration(**_base_params(analytics={"enabled": True, "entities": []}))
    assert "entity" in str(exc.value).lower()


def test_analytics_enabled_without_metric_groups_raises():
    with pytest.raises(UserException):
        Configuration(**_base_params(analytics={"enabled": True, "entities": ["LINE_ITEM"], "metric_groups": []}))


def test_analytics_custom_range_requires_dates():
    with pytest.raises(UserException):
        Configuration(
            **_base_params(
                analytics={
                    "enabled": True,
                    "entities": ["LINE_ITEM"],
                    "date_range_mode": "custom",
                }
            )
        )


def test_analytics_defaults_when_disabled():
    cfg = Configuration(**_base_params())
    assert cfg.analytics.enabled is False
    assert cfg.analytics.date_range_mode == DateRangeMode.last_n_days
    assert cfg.analytics.n_days == 7


def test_n_days_out_of_range_raises():
    with pytest.raises(UserException):
        Configuration(**_base_params(analytics={"enabled": True, "entities": ["LINE_ITEM"], "n_days": 999}))
