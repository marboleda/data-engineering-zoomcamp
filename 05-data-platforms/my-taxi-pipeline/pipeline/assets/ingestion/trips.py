"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

@bruin"""


import os
import json
import pandas as pd
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.

def month_range(start_date, end_date):
  """Yield (year, month) tuples from start_date to end_date inclusive."""
  current = start_date.replace(day=1)
  while current <= end_date:
    yield current.year, current.month
    current += relativedelta(months=1)

def materialize():
  # Get date window from environment
  start_date = datetime.strptime(os.environ["BRUIN_START_DATE"], "%Y-%m-%d")
  end_date = datetime.strptime(os.environ["BRUIN_END_DATE"], "%Y-%m-%d")

  # Get pipeline variables
  vars_json = os.environ.get("BRUIN_VARS", "{}")
  pipeline_vars = json.loads(vars_json)
  taxi_types = pipeline_vars.get("taxi_types", ["yellow"])

  dfs = []
  for taxi_type in taxi_types:
    for year, month in month_range(start_date, end_date):
      file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
      url = BASE_URL + file_name
      try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        with open("./tmp/tmp_taxi.parquet", "wb") as f:
          f.write(resp.content)
        df = pd.read_parquet("./tmp/tmp_taxi.parquet")
        df["taxi_type"] = taxi_type
        df["extracted_at"] = datetime.utcnow().isoformat()
        dfs.append(df)
      except Exception as e:
        print(f"Warning: Could not fetch {url}: {e}")
  if not dfs:
    raise RuntimeError("No data ingested for the given window and taxi_types.")
  final_df = pd.concat(dfs, ignore_index=True)
  return final_df


