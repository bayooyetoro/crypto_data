# !/bin/bash

curl "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana&vs_currencies=usd" -o prices.json

if [ $? -ne 0 ]; then
    echo "Error: Failed to fetch data from CoinGecko API"
    exit 1
fi

