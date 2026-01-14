from airflow.models import Variable
import requests
from datetime import datetime, timezone
from airflow.hooks.base import BaseHook
import clickhouse_connect
import json


def main():
    
    print('Start')
    crypto_pairs = Variable.get("CRYPTO_PAIRS", deserialize_json=True)
    #COIN_ID = Variable.get("COIN_ID")
    #SYMBOL = Variable.get("SYMBOL")
    VS_CURRENCY = Variable.get("VS_CURRENCY")
    DAYS = Variable.get("DAYS")

    conn = BaseHook.get_connection("clickhouse_default")
    client = clickhouse_connect.get_client(
        host=conn.host,   
        port=conn.port,
        username=conn.login,
        password=conn.password,
        database=conn.schema
    )

    for pair in crypto_pairs:
        COIN_ID = pair["coin_id"]
        SYMBOL = pair["symbol"]
        print(f"I'm processing it: {SYMBOL} → {COIN_ID}")
       
        result = client.query("SELECT max(timestamp) FROM crypto.crypto_prices WHERE symbol = %s", parameters = [SYMBOL])
        last_ts = result.result_rows[0][0]
        if last_ts is not None:
            last_ts = last_ts.replace(tzinfo=timezone.utc)

        url = (
            f"https://api.coingecko.com/api/v3/coins/{COIN_ID}/market_chart"
            f"?vs_currency={VS_CURRENCY}&days={DAYS}"
        )

        data = requests.get(url).json()
        print('data received')

        rows = []
        for ts_ms, price in data["prices"]:
            ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            if last_ts is None or ts > last_ts:
                rows.append((SYMBOL, float(price), ts, datetime.now(timezone.utc)))

        print(f'{SYMBOL}: rows to insert: {len(rows)}')

        if rows:
            client.insert(
                table='crypto_prices',
                data=rows,
                column_names=['symbol', 'price_usd', 'timestamp', 'loaded_at']
            )
            print(f"Inserted {len(rows)} rows for {SYMBOL} into ClickHouse")
        else:
            print(f"No new data to insert for {SYMBOL}")

    print('Finish')
