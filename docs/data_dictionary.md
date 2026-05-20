# Data Dictionary

## Bronze Layer

### bronze/nyc_taxi/*.parquet
- `tpep_pickup_datetime`: pickup timestamp (UTC-normalized in Silver).
- `tpep_dropoff_datetime`: dropoff timestamp.
- `PULocationID`: pickup zone ID.
- `DOLocationID`: dropoff zone ID.
- `passenger_count`: number of passengers.
- `trip_distance`: trip length (miles).
- `fare_amount`: fare in USD.
- `total_amount`: total trip charge in USD.

### bronze/openaq/openaq_raw.json
- `results[]`: flattened OpenAQ measurements pulled from API v3 (`hours`) using real source data.
- key fields: `timestamp_utc`, `parameter`, `value`, `unit`, `sensor_id`, `location_id`, `city`.

### bronze/economy/*
- `gdp_world_bank_raw.json`: World Bank GDP API payload.
- `fx_ecb_raw.csv`: ECB USD/EUR daily rate payload.

### bronze/weather/weather_raw_*.json
- hourly weather payload from Open-Meteo.

## Silver Layer

### silver/nyc_taxi/taxi_trips_clean.parquet
- cleaned, deduplicated taxi events with normalized timestamps.

### silver/openaq/air_quality_measurements.parquet
- normalized air-quality measurements.
- key columns:
  - `timestamp_utc`, `timestamp_local`
  - `parameter` (e.g., `pm25`, `no2`, `o3`)
  - `value`, `unit`
  - `location`, `location_id`, `sensor_id`, `city`, `country`
  - `latitude`, `longitude`

### silver/economy/*.parquet
- `gdp.parquet`: annual GDP by year/country.
- `fx_rates.parquet`: daily USD/EUR exchange rates.

### silver/weather/weather_enriched.parquet
- hourly weather with enrichment:
  - `temperature_c`, `humidity_pct`, `apparent_temperature_c`
  - `precipitation_mm`, `cloud_cover_pct`, `wind_speed_kmh`
  - `weather_code`, `weather_label`, `comfort_index`, `is_raining`

## Gold Layer

### gold/nyc_taxi/fact_taxi_daily.parquet
- daily mobility/economics by pickup zone.
- `pickup_date`, `PULocationID`, `trip_count`, `passenger_total`, `revenue_usd`, `avg_trip_distance_miles`.

### gold/openaq/fact_air_quality_daily.parquet
- daily pollutant stats by city and pollutant.
- `date`, `city`, `parameter`, `avg_pollutant_value`, `max_pollutant_value`, `measurement_count`.

### gold/economy/fact_fx_gdp_daily.parquet
- macro + FX context.
- `fx_date`, `usd_to_eur_rate`, `year`, `gdp_usd`, `gdp_eur_approx`.

### gold/weather/*
- `fact_weather_hourly.parquet`: enriched hourly weather time series.
- `fact_weather_daily.parquet`: daily weather aggregates per city.
