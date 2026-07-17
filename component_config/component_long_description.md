The X Ads extractor pulls advertising data from the [X Ads API](https://docs.x.com/x-ads-api) (formerly
Twitter Ads) into Keboola Storage, so you can build ad reporting and blend it with the rest of your data.

## Features

- **Campaign-management entities** — extract accounts, funding instruments, campaigns, line items,
  promoted tweets, promoted accounts and media creatives. Pick exactly which objects you need.
- **Performance analytics** — pull metrics (engagement, billing, video, conversions, ...) via the
  asynchronous analytics jobs, at daily, hourly or total granularity, for the entity levels you choose.
- **Incremental loading** — output tables are upserted on their primary key and the extractor keeps a
  watermark of the last run, so scheduled runs only fetch what changed.
- **Multiple accounts** — extract from several X Ads accounts in a single configuration.

## Authentication

The X Ads API requires **OAuth 1.0a** user-context authentication. Provide the four credentials from your
X developer App (Consumer API Key & Secret, Access Token & Secret). The access token is long-lived, so you
generate it once and paste it into the configuration — there is no interactive login at run time.

> Your developer App must be **approved for the Ads API** by X before any endpoint will return data.
