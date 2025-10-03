#!/bin/bash

DB_FILE="crypto_prices.db"

COINS="bitcoin,ethereum,solana"
CURRENCIES="usd"
API_URL="https://api.coingecko.com/api/v3/simple/price?ids=${COINS}&vs_currencies=${CURRENCIES}"

MAX_RETRIES=2
RETRY_INTERVAL=30 


setup_db() {
    echo "--- Setting up database schema..."
    SQL_SCHEMA="
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            crypto_id TEXT NOT NULL,
            price_usd REAL NOT NULL,
            ingestion_timestamp TEXT NOT NULL
        );
    "
    # Use ':memory:' if the DB file doesn't exist to ensure sqlite3 runs, then redirect
    echo "$SQL_SCHEMA" | sqlite3 "$DB_FILE"
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to set up database schema."
        return 1
    fi
    echo "--- Database setup complete."
    return 0
}


run_etl() {
    echo "--- Starting ETL process..."
    local timestamp=$(date -u +"%Y-%m-%d %H:%M:%S")
    local raw_data
    local sql_insert_statements=""

    #  -s' for silent mode, '-S' to show errors, and '-L' for following redirects.
    raw_data=$(curl -sSL -w "%{http_code}" "$API_URL")
    
    local http_code="${raw_data: -3}"
    local json_data="${raw_data:0:$((${#raw_data}-3))}"

 
    if [ "$http_code" -ne 200 ]; then
        echo "ERROR: API call failed with HTTP code $http_code."
        return 1
    fi


    if ! command -v jq &> /dev/null; then
        echo "ERROR: 'jq' is required but not installed. Please install jq."
        return 1
    fi


    local parsing_output
    parsing_output=$(echo "$json_data" | jq -r 'to_entries[] | .key, .value.usd')
    
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to parse JSON data using jq."
        return 1
    fi

   
    local crypto_id
    local price_usd
    local line_count=0
    
    while IFS= read -r crypto_id; do
        IFS= read -r price_usd || break # Read the next line for price

    
        if [[ -z "$crypto_id" || -z "$price_usd" || ! "$price_usd" =~ ^[0-9]+\.?[0-9]*$ ]]; then
            echo "WARN: Skipping invalid data point: ID='$crypto_id', Price='$price_usd'"
            continue
        fi

      
        sql_insert_statements+="
            INSERT INTO prices (crypto_id, price_usd, ingestion_timestamp)
            VALUES ('$crypto_id', $price_usd, '$timestamp');
        "
        line_count=$((line_count + 1))
    done <<< "$parsing_output"

    if [ $line_count -eq 0 ]; then
        echo "ERROR: No valid data points were extracted."
        return 1
    fi


    echo "--- Inserting $line_count records into $DB_FILE..."
    echo "$sql_insert_statements" | sqlite3 "$DB_FILE"
    
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to insert data into SQLite."
        return 1
    fi
    
    echo "--- ETL completed successfully."
    return 0
}


# Handling retries
if ! setup_db; then
    exit 1
fi

attempt=0
while [ $attempt -le $MAX_RETRIES ]; do
    if run_etl; then
        exit 0
    else
        if [ $attempt -lt $MAX_RETRIES ]; then
            echo "RETRYING: Attempt $((attempt + 1)) failed. Retrying in $RETRY_INTERVAL seconds..."
            sleep "$RETRY_INTERVAL"
        else
            echo "FATAL: All $MAX_RETRIES attempts failed. Halting pipeline execution."
            exit 1
        fi
        attempt=$((attempt + 1))
    fi
done

exit 1 # safety net