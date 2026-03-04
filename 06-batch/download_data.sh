set -e

TAXI_TYPE=$1 # "yellow"
YEAR=$2 # 2020

for MONTH in {01..12}; do
    URL="https://d37ci6vzurychx.cloudfront.net/trip-data/${TAXI_TYPE}_tripdata_${YEAR}-${MONTH}.parquet"
    LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${MONTH}"
    LOCAL_FILE=${TAXI_TYPE}_tripdata_${YEAR}-${MONTH}.parquet
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

    echo "downloading ${URL} to ${LOCAL_PATH}"
    mkdir -p $LOCAL_PREFIX
    curl -o $LOCAL_PATH $URL
done