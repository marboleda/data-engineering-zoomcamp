/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

# TODO: Choose time-based incremental processing if the dataset is naturally time-windowed.
# - This module expects you to use `time_interval` to reprocess only the requested window.
materialization:
  type: table

# TODO: Add one custom check that validates a staging invariant (uniqueness, ranges, etc.)
# Docs: https://getbruin.com/docs/bruin/quality/custom
custom_checks:
  - name: row_count_positive
    description: Ensures the table is not empty
    query: SELECT COUNT(*) > 0 FROM staging.trips
    value: 1

@bruin */

WITH cleaned AS (
  SELECT
    DISTINCT ON (
      vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, pu_location_id, do_location_id, payment_type, fare_amount
    )
      vendor_id,
      tpep_pickup_datetime AS pickup_datetime,
      tpep_dropoff_datetime AS dropoff_datetime,
      COALESCE(passenger_count, 1) AS passenger_count,
      trip_distance,
      ratecode_id,
      store_and_fwd_flag,
      pu_location_id,
      do_location_id,
      payment_type,
      fare_amount,
      extra,
      mta_tax,
      tip_amount,
      tolls_amount,
      improvement_surcharge,
      total_amount,
      congestion_surcharge,
      airport_fee,
      taxi_type,
      extracted_at
  FROM ingestion.trips
  WHERE
    vendor_id IS NOT NULL
    AND tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND pu_location_id IS NOT NULL
    AND do_location_id IS NOT NULL
    AND payment_type IS NOT NULL
    AND fare_amount >= 0
    AND trip_distance >= 0
    AND passenger_count >= 0
    AND total_amount >= 0
)
SELECT
  c.vendor_id,
  c.pickup_datetime,
  c.dropoff_datetime,
  c.passenger_count,
  c.trip_distance,
  c.ratecode_id,
  c.store_and_fwd_flag,
  c.pu_location_id,
  c.do_location_id,
  c.payment_type,
  pl.payment_type_name,
  c.fare_amount,
  c.extra,
  c.mta_tax,
  c.tip_amount,
  c.tolls_amount,
  c.improvement_surcharge,
  c.total_amount,
  c.congestion_surcharge,
  c.airport_fee,
  c.taxi_type,
  c.extracted_at
FROM cleaned c
LEFT JOIN ingestion.payment_lookup pl
  ON c.payment_type = pl.payment_type_id;

