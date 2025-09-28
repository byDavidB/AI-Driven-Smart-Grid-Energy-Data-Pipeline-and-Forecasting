-- infra/init.sql
-- Purpose: Create Bronze (raw), Silver (fact), Gold (mart) tables for GridMatch.
-- Convention: store all timestamps as UTC (TIMESTAMPTZ).

-- =========================
-- Bronze (raw) layer
-- =========================
CREATE TABLE IF NOT EXISTS raw_weather (
  site         TEXT                     NOT NULL,
  ts_utc       TIMESTAMPTZ              NOT NULL,
  ghi_wm2      DOUBLE PRECISION         CHECK (ghi_wm2 >= 0 OR ghi_wm2 IS NULL),
  t2m_c        DOUBLE PRECISION         CHECK (t2m_c   BETWEEN -80 AND 80 OR t2m_c IS NULL),
  ws10_mps     DOUBLE PRECISION         CHECK (ws10_mps >= 0 OR ws10_mps IS NULL),
  raw_json     JSONB,
  ingested_at  TIMESTAMPTZ              NOT NULL DEFAULT NOW(),
  CONSTRAINT pk_raw_weather PRIMARY KEY (site, ts_utc)
);
CREATE INDEX IF NOT EXISTS idx_raw_weather_site_ts ON raw_weather (site, ts_utc);

-- Why: raw holds exactly what came from the API (plus parsed columns).
-- We keep JSONB for traceability; PK prevents dupes per hour.

-- =========================
-- Silver (fact) layer
-- =========================
CREATE TABLE IF NOT EXISTS fact_weather (
  site         TEXT            NOT NULL,
  ts_utc       TIMESTAMPTZ     NOT NULL,
  ghi_wm2      DOUBLE PRECISION NOT NULL CHECK (ghi_wm2 >= 0),
  temp_c       DOUBLE PRECISION NOT NULL CHECK (temp_c BETWEEN -80 AND 80),
  wind_mps     DOUBLE PRECISION NOT NULL CHECK (wind_mps >= 0),
  cleaned_at   TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
  CONSTRAINT pk_fact_weather PRIMARY KEY (site, ts_utc),
  -- enforce hourly alignment (ts is top of the hour)
  CONSTRAINT ck_fact_weather_hourly CHECK (date_trunc('hour', ts_utc) = ts_utc)
);
CREATE INDEX IF NOT EXISTS idx_fact_weather_site_ts ON fact_weather (site, ts_utc);

-- Why: one clean, hourly row per site with strict ranges and hourly binning.

-- =========================
-- Gold (features)
-- =========================
CREATE TABLE IF NOT EXISTS mart_features (
  site          TEXT             NOT NULL,
  ts_utc        TIMESTAMPTZ      NOT NULL,
  ghi_kwh_m2    DOUBLE PRECISION NOT NULL CHECK (ghi_kwh_m2 >= 0),
  pv_est_mwh    DOUBLE PRECISION NOT NULL CHECK (pv_est_mwh   >= 0),
  wind_est_mwh  DOUBLE PRECISION NOT NULL CHECK (wind_est_mwh >= 0),
  computed_at   TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
  CONSTRAINT pk_mart_features PRIMARY KEY (site, ts_utc),
  CONSTRAINT ck_mart_features_hourly CHECK (date_trunc('hour', ts_utc) = ts_utc)
);
CREATE INDEX IF NOT EXISTS idx_mart_features_site_ts ON mart_features (site, ts_utc);

-- Why: features translate weather -> estimated PV/Wind MWh (supply proxy).

-- =========================
-- Gold (forecast)
-- =========================
CREATE TABLE IF NOT EXISTS mart_forecast (
  site          TEXT             NOT NULL,
  ts_utc        TIMESTAMPTZ      NOT NULL,  -- timestamp being predicted
  model         TEXT             NOT NULL CHECK (model IN ('baseline','sarimax')),
  var           TEXT             NOT NULL CHECK (var   IN ('pv','wind')),
  horizon_h     SMALLINT         NOT NULL CHECK (horizon_h BETWEEN 1 AND 48),
  yhat          DOUBLE PRECISION NOT NULL CHECK (yhat >= 0),
  yhat_lower    DOUBLE PRECISION,
  yhat_upper    DOUBLE PRECISION,
  created_at    TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
  CONSTRAINT pk_mart_forecast PRIMARY KEY (site, ts_utc, model, var, horizon_h)
);
CREATE INDEX IF NOT EXISTS idx_mart_forecast_site_ts ON mart_forecast (site, ts_utc);

-- Why: holds 24 (or up to 48) hourly forecasts per var/model, per site, with CIs.

-- =========================
-- Gold (KPIs)
-- =========================
CREATE TABLE IF NOT EXISTS mart_kpis (
  site               TEXT             NOT NULL,
  ts_utc             TIMESTAMPTZ      NOT NULL,
  pv_capacity_mw     DOUBLE PRECISION NOT NULL CHECK (pv_capacity_mw   >= 0),
  wind_capacity_mw   DOUBLE PRECISION NOT NULL CHECK (wind_capacity_mw >= 0),
  pv_cf              DOUBLE PRECISION NOT NULL CHECK (pv_cf   BETWEEN 0 AND 1.2),
  wind_cf            DOUBLE PRECISION NOT NULL CHECK (wind_cf BETWEEN 0 AND 1.2),
  computed_at        TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
  CONSTRAINT pk_mart_kpis PRIMARY KEY (site, ts_utc),
  CONSTRAINT ck_mart_kpis_hourly CHECK (date_trunc('hour', ts_utc) = ts_utc)
);
CREATE INDEX IF NOT EXISTS idx_mart_kpis_site_ts ON mart_kpis (site, ts_utc);

-- Why: CF (capacity factor) is the business-friendly metric recruiters understand.
