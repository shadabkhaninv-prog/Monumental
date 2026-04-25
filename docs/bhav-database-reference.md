# Bhav Database Reference

This document summarizes the MySQL `bhav` database tables that are used by the programs in this repository.

It is meant as a practical reference for people who want to read from or write to the local BHAV store without having to inspect every script.

## Database Overview

- Database name: `bhav`
- Default connection in the codebase:
  - host: `localhost`
  - port: `3306`
  - user: `root`
  - password: `root`
- Yearly market data is stored in sharded tables named `bhavYYYY` such as `bhav2024`, `bhav2025`, and `bhav2026`.
- Most apps query the latest trading date via `mktdatecalendar`.
- Several helper/reference tables support symbol mapping, sector lookup, index history, IPO metadata, and quarterly fundamentals.

## Table Catalog

### 1. `bhavYYYY`

Examples: `bhav2024`, `bhav2025`, `bhav2026`

Purpose:
- Main daily BHAV/equity price history, split by calendar year.
- Used for charts, screeners, turnover calculations, price lookup, and date-window scans.

Write behavior:
- These tables are read heavily by the apps.
- They are populated outside this repo by the bhav import pipeline / SQL batch.

Observed columns used by programs:

| Column | Type expectation | Usage |
|---|---|---|
| `mktdate` | `DATE` | Trading date, used for date filtering and ordering |
| `symbol` | `VARCHAR` / text | Stock symbol, usually upper-cased in queries |
| `open` | numeric | OHLC calculations and chart rendering |
| `high` | numeric | OHLC calculations and 52-week / intraday analytics |
| `low` | numeric | OHLC calculations |
| `close` | numeric | Main reference price, turnover calculations, return math |
| `prevclose` | numeric | Day-over-day change calculations |
| `volume` | numeric | Liquidity / turnover calculations |
| `deliveryvolume` | numeric | Delivery percentage calculations |
| `VOLATILITY` | numeric | Screener volatility filters |
| `closeindictor` | numeric / flag | Used in viewer calculations |
| `20DMA` | numeric | Moving-average analytics |
| `10dma` | numeric | Moving-average analytics |
| `50dma` | numeric | Moving-average analytics |
| `5dma` | numeric | Moving-average analytics |

Programs that use it:
- [`app.py`](../app.py)
- [`bhav_screener.py`](../bhav_screener.py)
- [`neo_liquid_momentum_scanner.py`](../neo_liquid_momentum_scanner.py)
- [`trade_plan_server.py`](../trade_plan_server.py)
- [`stock_rating.py`](../stock_rating.py)
- [`screener_top_sales_yoy.py`](../screener_top_sales_yoy.py)
- [`atr_report.py`](../atr_report.py)

Notes:
- Some code uses `UPPER(symbol)` for matching, so symbol case is not assumed.
- The viewer and scanner code treat the yearly tables as a single logical history stream.

### 2. `mktdatecalendar`

Purpose:
- Trading-day calendar.
- Used to find the latest trading date and to build trading-day windows such as “last 10 trading days” or “last 21 trading days”.

Observed columns used by programs:

| Column | Type expectation | Usage |
|---|---|---|
| `mktdate` | `DATE` | Distinct trading-day value |

Programs that use it:
- [`app.py`](../app.py)
- [`bhav_screener.py`](../bhav_screener.py)

### 3. `sectors`

Purpose:
- Symbol-to-sector reference table.
- Also contains a few sector columns that are used to build lists and sector mappings.

Observed columns used by programs:

| Column | Type expectation | Usage |
|---|---|---|
| `symbol` | text | Join key to BHAV symbols |
| `sector1` | text | Primary sector mapping, universe filtering, export |
| `sector2` | text | Additional sector label for UI filtering |
| `sector3` | text | Additional sector label for UI filtering |

Programs that use it:
- [`app.py`](../app.py)
- [`bhav_screener.py`](../bhav_screener.py)
- [`neo_liquid_momentum_scanner.py`](../neo_liquid_momentum_scanner.py)
- [`neo_top_sector_scanner.py`](../neo_top_sector_scanner.py)
- [`stock_rating.py`](../stock_rating.py)
- [`build_sector_csv.py`](../build_sector_csv.py)

### 4. `indexbhav`

Purpose:
- Daily index history, especially Nifty Smallcap 100 and 250.

Observed columns used by programs:

| Column | Type expectation | Usage |
|---|---|---|
| `symbol` | text | Index name match |
| `mktdate` | `DATE` | Date range filter |
| `open` | numeric | Index OHLC |
| `high` | numeric | Index OHLC |
| `low` | numeric | Index OHLC |
| `close` | numeric | Index OHLC |
| `diff` | numeric | Percent change / daily change |

Programs that use it:
- [`update_indexbhav_smallcaps.py`](../update_indexbhav_smallcaps.py)
- [`stock_rating.py`](../stock_rating.py)
- [`stock_rating - Copy.py`](../stock_rating%20-%20Copy.py)
- [`neo_liquid_momentum_scanner.py`](../neo_liquid_momentum_scanner.py)
- [`outperformance_analysis.py`](../outperformance_analysis.py)
- [`performance_tracker.py`](../performance_tracker.py)
- [`app.py`](../app.py) indirectly through the viewer and report flows

### 5. `nse_symbols`

Purpose:
- NSE symbol master / company-name mapping used for symbol resolution.

Exact DDL in repo:

```sql
CREATE TABLE nse_symbols (
    SYMBOL       VARCHAR(32)  NOT NULL,
    COMPANY_NAME VARCHAR(255) NOT NULL,
    PRIMARY KEY (SYMBOL)
)
```

Programs that use it:
- [`load_nse_symbols.py`](../load_nse_symbols.py)
- [`app.py`](../app.py)
- [`trade_plan_server.py`](../trade_plan_server.py)
- [`stock_rating.py`](../stock_rating.py)
- [`load_ipo_csv.py`](../load_ipo_csv.py)
- [`screener_top_sales_yoy.py`](../screener_top_sales_yoy.py) indirectly via screening workflows

### 6. `inactive_symbols`

Purpose:
- Mapping of old/inactive symbols to replacement symbols.
- Used when a symbol changed name, merged, or otherwise moved to a newer ticker.

Exact DDL in repo:

```sql
CREATE TABLE IF NOT EXISTS inactive_symbols (
    symbol VARCHAR(50) NOT NULL,
    new_symbol VARCHAR(50) NULL,
    PRIMARY KEY (symbol)
)
```

Observed columns used by programs:

| Column | Type expectation | Usage |
|---|---|---|
| `symbol` | text | Old/inactive symbol |
| `new_symbol` | text | Replacement active symbol |

Programs that use it:
- [`stock_rating.py`](../stock_rating.py)
- [`trade_plan_server.py`](../trade_plan_server.py)
- [`screener_top_sales_yoy.py`](../screener_top_sales_yoy.py)

### 7. `ipobhav`

Purpose:
- IPO reference/history table.
- Holds listed IPO symbol, listing date, listing open/close, and issue price.

Exact DDL in repo:

```sql
CREATE TABLE `ipobhav` (
    `SYMBOL` VARCHAR(32) NOT NULL,
    `LISTING_DATE` DATE NOT NULL,
    `LISTING_OPEN` DOUBLE DEFAULT NULL,
    `LISTING_CLOSE` DOUBLE DEFAULT NULL,
    `ISSUE_PRICE` DOUBLE DEFAULT NULL,
    PRIMARY KEY (`SYMBOL`, `LISTING_DATE`),
    KEY `idx_listing_date` (`LISTING_DATE`)
)
```

Programs that use it:
- [`load_ipo_csv.py`](../load_ipo_csv.py)
- [`chittorgarh_ipo_loader.py`](../chittorgarh_ipo_loader.py)
- [`moneycontrol_mainline_ipo_loader.py`](../moneycontrol_mainline_ipo_loader.py)
- [`stock_rating.py`](../stock_rating.py)
- [`app.py`](../app.py) indirectly through analytics and reference flows

### 8. `quarterly_fundamentals`

Purpose:
- Cached quarterly sales/profit data sourced from BSE XBRL / Screener workflows.

Exact DDL in repo:

```sql
CREATE TABLE IF NOT EXISTS quarterly_fundamentals (
  idquarterly_fundamentals INT(11) NOT NULL AUTO_INCREMENT,
  SYMBOL VARCHAR(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  SCREENER_SYMBOL VARCHAR(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  QUARTER_LABEL VARCHAR(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  QUARTER_END DATE DEFAULT NULL,
  SALES DOUBLE DEFAULT NULL,
  SALES_YOY_PCT DOUBLE DEFAULT NULL,
  PROFIT DOUBLE DEFAULT NULL,
  PROFIT_YOY_PCT DOUBLE DEFAULT NULL,
  STATEMENT_USED VARCHAR(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  SOURCE_URL VARCHAR(1024) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  LOAD_SOURCE VARCHAR(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  LAST_REFRESHED_AT DATETIME DEFAULT NULL,
  PRIMARY KEY (idquarterly_fundamentals),
  UNIQUE KEY SYMBOL_QUARTER (SYMBOL, QUARTER_END),
  KEY SCREENER_SYMBOL_QUARTER (SCREENER_SYMBOL, QUARTER_END),
  KEY QUARTER_END (QUARTER_END)
)
```

Programs that use it:
- [`bse_quarterly_cache_loader.py`](../bse_quarterly_cache_loader.py)
- [`screener_top_sales_yoy.py`](../screener_top_sales_yoy.py)

### 9. `splits`

Purpose:
- Corporate-action split reference data.

Observed columns used by programs:

| Column | Type expectation | Usage |
|---|---|---|
| `Symbol` / `symbol` | text | Join key |
| `CompanyName` | text | Display/reference label |
| `ExDate` | date-like text | Corporate-action effective date |
| `A` | text / numeric | Split metadata |
| `B` | text / numeric | Split metadata |
| `Ratio` | numeric | Adjustment ratio |

Programs that use it:
- [`app.py`](../app.py)

### 10. `bonus`

Purpose:
- Corporate-action bonus reference data.

Observed columns used by programs:

| Column | Type expectation | Usage |
|---|---|---|
| `Symbol` / `symbol` | text | Join key |
| `CompanyName` | text | Display/reference label |
| `ExDate` | date-like text | Corporate-action effective date |
| `A` | text / numeric | Bonus metadata |
| `B` | text / numeric | Bonus metadata |
| `Ratio` | numeric | Adjustment ratio |

Programs that use it:
- [`app.py`](../app.py)

### 11. `gmlistarchive`

Purpose:
- Output table produced by the bhav SQL batch.
- Used to export the GMList universe for a given cutoff date.

Observed columns used by programs:

| Column | Type expectation | Usage |
|---|---|---|
| `cutoff` | date | Batch date filter |
| `symbol` | text | Exported into `gmlist_*.txt` as `NSE:<symbol>` |

Programs that use it:
- [`run_bhav_sql_batch.py`](../run_bhav_sql_batch.py)

## Practical Notes

### Yearly table convention

The code treats `bhavYYYY` as the primary market-data partitioning scheme. When a script needs a date range that crosses years, it builds a `UNION ALL` across the relevant yearly tables.

### Symbol matching

Several scripts normalize symbols with `UPPER(...)` or remove punctuation to improve matching. This is especially important for:

- `trade_plan_server.py`
- `stock_rating.py`
- `app.py`

### Liquidity calculations

Turnover is typically computed as:

```text
turnover = close * volume
```

Some scripts use a 21-trading-day or 42-trading-day rolling window.

### If you need exact live schema

This document is based on the tables and columns referenced in the repository source. If the live MySQL schema has drifted, confirm it directly with:

```sql
DESCRIBE bhav2026;
DESCRIBE mktdatecalendar;
DESCRIBE sectors;
DESCRIBE indexbhav;
DESCRIBE nse_symbols;
DESCRIBE inactive_symbols;
DESCRIBE ipobhav;
DESCRIBE quarterly_fundamentals;
DESCRIBE splits;
DESCRIBE bonus;
DESCRIBE gmlistarchive;
```

## Source Files Reviewed

- [`app.py`](../app.py)
- [`bhav_screener.py`](../bhav_screener.py)
- [`trade_plan_server.py`](../trade_plan_server.py)
- [`neo_liquid_momentum_scanner.py`](../neo_liquid_momentum_scanner.py)
- [`stock_rating.py`](../stock_rating.py)
- [`update_indexbhav_smallcaps.py`](../update_indexbhav_smallcaps.py)
- [`load_nse_symbols.py`](../load_nse_symbols.py)
- [`load_ipo_csv.py`](../load_ipo_csv.py)
- [`chittorgarh_ipo_loader.py`](../chittorgarh_ipo_loader.py)
- [`moneycontrol_mainline_ipo_loader.py`](../moneycontrol_mainline_ipo_loader.py)
- [`bse_quarterly_cache_loader.py`](../bse_quarterly_cache_loader.py)
- [`screener_top_sales_yoy.py`](../screener_top_sales_yoy.py)
- [`run_bhav_sql_batch.py`](../run_bhav_sql_batch.py)
