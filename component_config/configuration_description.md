## Authentication

Provide the four OAuth 1.0a credentials from your X developer App (which must be approved for the Ads API):

- **Consumer API Key** / **Consumer API Secret** — your App's keys.
- **Access Token** / **Access Token Secret** — generated for a user with access to the ad account(s).

Use **Test Connection** to verify the credentials.

## What to extract

- **Ad Accounts** — select the X Ads account(s) to extract from.
- **Objects to extract** — the campaign-management entities you want (campaigns, line items, promoted
  tweets, ...). Each becomes an output table `x_ads_<object>`.
- **Analytics** — enable to pull performance metrics. Choose the entity levels, metric groups,
  granularity and date range. Each entity level becomes an output table `x_ads_stats_<entity>`.

## Load type

- **incremental_load** (default) — upserts on the primary key and only fetches data since the last run.
- **full_load** — overwrites the output tables on every run.
