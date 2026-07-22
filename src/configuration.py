"""Configuration models for the X Ads extractor.

All configuration is validated up-front via Pydantic; any validation error is
re-raised as a ``UserException`` so the platform surfaces it as a user error
(exit 1) instead of an unexpected crash (exit 2).
"""

from __future__ import annotations

from enum import StrEnum

from keboola.component.exceptions import UserException
from pydantic import BaseModel, ConfigDict, Field, ValidationError, computed_field, model_validator


class EntityObject(StrEnum):
    """Campaign-management entity endpoints that can be extracted."""

    accounts = "accounts"
    funding_instruments = "funding_instruments"
    campaigns = "campaigns"
    line_items = "line_items"
    promoted_tweets = "promoted_tweets"
    promoted_accounts = "promoted_accounts"
    media_creatives = "media_creatives"


class StatsEntity(StrEnum):
    """Entity types supported by the analytics (stats) endpoints."""

    ACCOUNT = "ACCOUNT"
    FUNDING_INSTRUMENT = "FUNDING_INSTRUMENT"
    CAMPAIGN = "CAMPAIGN"
    LINE_ITEM = "LINE_ITEM"
    PROMOTED_TWEET = "PROMOTED_TWEET"
    PROMOTED_ACCOUNT = "PROMOTED_ACCOUNT"


class MetricGroup(StrEnum):
    ENGAGEMENT = "ENGAGEMENT"
    BILLING = "BILLING"
    VIDEO = "VIDEO"
    MEDIA = "MEDIA"
    WEB_CONVERSION = "WEB_CONVERSION"
    MOBILE_CONVERSION = "MOBILE_CONVERSION"
    LIFE_TIME_VALUE_MOBILE_CONVERSION = "LIFE_TIME_VALUE_MOBILE_CONVERSION"


class Granularity(StrEnum):
    DAY = "DAY"
    TOTAL = "TOTAL"
    HOUR = "HOUR"


class Placement(StrEnum):
    ALL_ON_TWITTER = "ALL_ON_TWITTER"
    PUBLISHER_NETWORK = "PUBLISHER_NETWORK"


class DateRangeMode(StrEnum):
    last_n_days = "last_n_days"
    custom = "custom"


class LoadType(StrEnum):
    full_load = "full_load"
    incremental_load = "incremental_load"


class Analytics(BaseModel):
    """Analytics (async stats jobs) sub-configuration."""

    model_config = ConfigDict(extra="ignore")

    enabled: bool = False
    entities: list[StatsEntity] = Field(default_factory=list)
    metric_groups: list[MetricGroup] = Field(default_factory=lambda: [MetricGroup.ENGAGEMENT, MetricGroup.BILLING])
    granularity: Granularity = Granularity.DAY
    placement: Placement = Placement.ALL_ON_TWITTER
    date_range_mode: DateRangeMode = DateRangeMode.last_n_days
    # Used when date_range_mode == last_n_days
    n_days: int = Field(default=7, ge=1, le=90)
    # Used when date_range_mode == custom (ISO date, YYYY-MM-DD)
    start_date: str | None = None
    end_date: str | None = None

    @model_validator(mode="after")
    def _validate(self) -> Analytics:
        if not self.enabled:
            return self
        if not self.entities:
            raise ValueError("At least one analytics entity must be selected when analytics is enabled.")
        if not self.metric_groups:
            raise ValueError("At least one metric group must be selected when analytics is enabled.")
        if self.date_range_mode == DateRangeMode.custom and not (self.start_date and self.end_date):
            raise ValueError("start_date and end_date are required when date range mode is 'custom'.")
        return self


class Credentials(BaseModel):
    """The four OAuth 1.0a secrets, parsed on their own.

    The sync-action path (Test Connection / Load Accounts) only needs credentials,
    so it validates just this model — an unrelated analytics validation error can
    never block a credential test.
    """

    # Ignore unknown keys: the platform injects extras (e.g. `debug`) that aren't
    # part of this model and must not fail validation. ``populate_by_name`` lets
    # callers pass either the ``#``-aliased key or the plain field name.
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    consumer_key: str = Field(alias="#consumer_key")
    consumer_secret: str = Field(alias="#consumer_secret")
    access_token: str = Field(alias="#access_token")
    access_token_secret: str = Field(alias="#access_token_secret")

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{'.'.join(str(x) for x in err['loc'])}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Configuration validation error: {', '.join(error_messages)}") from e


class Configuration(Credentials):
    """Top-level component configuration."""

    account_ids: list[str] = Field(default_factory=list)
    objects: list[EntityObject] = Field(default_factory=list)
    analytics: Analytics = Field(default_factory=Analytics)
    load_type: LoadType = LoadType.incremental_load

    @computed_field
    @property
    def incremental(self) -> bool:
        return self.load_type == LoadType.incremental_load
