
/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks
name: reports.trips_report

type: bq.sql

depends:
  - staging.trips

materialization:
  type: table

# TODO: Define report columns + primary key(s) at your chosen level of aggregation.
columns:
  - name: taxi_type
    type: VARCHAR
    description: Type of taxi (yellow, green, etc.)
    primary_key: true
  - name: payment_type_name
    type: VARCHAR
    description: Payment method name
    primary_key: true
  - name: trip_count
    type: BIGINT
    description: Number of trips
    checks:
      - name: non_negative
  - name: total_distance
    type: DOUBLE
    description: Total trip distance
    checks:
      - name: non_negative
  - name: avg_distance
    type: DOUBLE
    description: Average trip distance
    checks:
      - name: non_negative
  - name: total_revenue
    type: DOUBLE
    description: Total revenue (fare + extras)
    checks:
      - name: non_negative
  - name: avg_revenue
    type: DOUBLE
    description: Average revenue per trip
    checks:
      - name: non_negative
  - name: total_tip
    type: DOUBLE
    description: Total tip amount
    checks:
      - name: non_negative
  - name: avg_tip
    type: DOUBLE
    description: Average tip amount
    checks:
      - name: non_negative
  - name: total_passengers
    type: BIGINT
    description: Total passengers
    checks:
      - name: non_negative
  - name: avg_passengers
    type: DOUBLE
    description: Average passengers per trip
    checks:
      - name: non_negative

@bruin */

SELECT
  taxi_type,
  payment_type_name,
  COUNT(*) AS trip_count,
  SUM(trip_distance) AS total_distance,
  AVG(trip_distance) AS avg_distance,
  SUM(total_amount) AS total_revenue,
  AVG(total_amount) AS avg_revenue,
  SUM(tip_amount) AS total_tip,
  AVG(tip_amount) AS avg_tip,
  SUM(passenger_count) AS total_passengers,
  AVG(passenger_count) AS avg_passengers
FROM staging.trips
GROUP BY taxi_type, payment_type_name
ORDER BY taxi_type, payment_type_name;
