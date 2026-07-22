### Authentication

Provide the four OAuth 1.0a credentials from your X developer App (which must be approved for the Ads API):

- **Consumer API Key** / **Consumer API Secret** — your App's keys.
- **Access Token** / **Access Token Secret** — generated for a user with access to the ad account(s).

Use **Test Connection** to verify the credentials.

- **Ad Accounts** — select the X Ads account(s) to extract from. This selection is shared by every row.

### Rows — one per extraction

This component is **row-based**: the authentication and account selection above live on the main
configuration, and each **row** defines one independent extraction that runs on its own (in row order,
with its own incremental state). A typical setup is one row for entities and another for analytics.

Each row has:

- **Objects to extract** — the campaign-management entities you want (campaigns, line items, promoted
  tweets, ...). Each becomes an output table `x_ads_<object>`.
- **Analytics** — enable to pull performance metrics. Choose the entity levels, metric groups,
  granularity and date range. Each entity level becomes an output table `x_ads_stats_<entity>`.
- **Load type**:
  - **incremental_load** (default) — upserts on the primary key and only fetches data since the last run.
  - **full_load** — overwrites the output tables on every run.

> Tip: because rows write to shared table names (`x_ads_<object>` / `x_ads_stats_<entity>`), avoid
> extracting the same object in two rows with **full_load** — the later row would overwrite the earlier.
> With **incremental_load** the rows safely upsert into the same table.
