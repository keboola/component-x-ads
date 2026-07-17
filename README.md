X Ads (Twitter Ads) Extractor
=============================

Extracts campaign-management entities and performance analytics from the
[X Ads API](https://docs.x.com/x-ads-api) (v12) into Keboola Storage.

**Table of Contents:**

[TOC]

Prerequisites
=============

You need an X developer App that is **approved for the Ads API**, and the four OAuth 1.0a
credentials it issues:

- Consumer API Key & Consumer API Secret (the App's keys)
- Access Token & Access Token Secret (for a user with access to the ad account)

The access token is long-lived, so it is generated once and pasted into the configuration —
there is no interactive login at run time.

Features
========

| **Feature**         | **Description**                                                      |
|---------------------|---------------------------------------------------------------------|
| Entities            | accounts, funding instruments, campaigns, line items, promoted tweets, promoted accounts, media creatives |
| Analytics           | asynchronous analytics jobs (engagement, billing, video, conversions) |
| Granularity         | DAY / HOUR / TOTAL                                                   |
| Multiple accounts   | extract several X Ads accounts in one configuration                  |
| Incremental Loading  | upsert on primary key + `last_run` watermark                        |
| Date Range Filter   | last-N-days or a custom start/end range                             |

Supported Endpoints
===================

Entity endpoints: `accounts`, `funding_instruments`, `campaigns`, `line_items`,
`promoted_tweets`, `promoted_accounts`, `media_creatives`. Analytics via the asynchronous
`stats/jobs` endpoint.

If you need additional endpoints, please submit your request to
[ideas.keboola.com](https://ideas.keboola.com/).

Configuration
=============

Authentication
--------------
`#consumer_key`, `#consumer_secret`, `#access_token`, `#access_token_secret` — the four OAuth 1.0a
credentials. Use **Test Connection** to validate them.

Data selection
--------------
- `account_ids` — the X Ads account(s) to extract from.
- `objects` — which entity objects to extract.
- `analytics` — enable and configure performance-metric extraction (entities, metric groups,
  granularity, placement, date range).
- `load_type` — `incremental_load` (default) or `full_load`.

Output
======

- One table per selected entity object: `x_ads_<object>` (primary key `id`, account-scoped
  objects also carry `account_id`).
- One analytics table per stats entity: `x_ads_stats_<entity>` (primary key
  `account_id, entity_type, entity_id, segment, placement, date`).

Development
-----------

To customize the local data folder path, replace the `CUSTOM_FOLDER` placeholder with your desired path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, initialize the workspace, and run the component using the following
commands:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone  component-ex-x-ads
cd component-ex-x-ads
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and perform lint checks using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For details about deployment and integration with Keboola, refer to the
[deployment section of the developer
documentation](https://developers.keboola.com/extend/component/deployment/).
