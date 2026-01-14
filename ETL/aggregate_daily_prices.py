from datetime import datetime, date, timedelta
from airflow.hooks.base import BaseHook
import clickhouse_connect



def main():
    print('Start')

    conn = BaseHook.get_connection("clickhouse_default")

    client = clickhouse_connect.get_client(
        host=conn.host,   
        port=conn.port,
        username=conn.login,
        password=conn.password,
        database=conn.schema
    )

    print('connection successful')

    result = client.query("SELECT max(date) FROM crypto.daily_avg_prices")
    last_date = result.result_rows[0][0]

    if last_date is not None and last_date < date(2020, 1, 1):
        print(f"Invalid last_date detected: {last_date}, switching to backfill")
        last_date = None

    if last_date is None:
        print("No aggregated data found — backfill mode")

        result = client.query('SELECT min(toDate(timestamp)) FROM crypto.crypto_prices')
        start_date = result.result_rows[0][0]

    else:
        start_date = last_date + timedelta(days=1)

    end_date = date.today() - timedelta(days=1)

    if start_date > end_date:
        print('No new days')
        return
    
    print(f'Aggregating from {start_date} to {end_date}')

    current_date = start_date

    while current_date <= end_date:
        print(f'Processing {current_date}')

        client.command(f"""
                       ALTER TABLE crypto.daily_avg_prices
                       DELETE WHERE date = toDate('{current_date}')
                       """)

        client.command(f"""
        INSERT INTO crypto.daily_avg_prices
        SELECT 
            toDate(timestamp) AS date,
            symbol,
            avg(price_usd) AS avg_price,
            now() AS calculated_at
        FROM crypto.crypto_prices
        WHERE toDate(timestamp) = toDate('{current_date}')
        GROUP BY date, symbol
        """)
    
        current_date += timedelta(days=1)

    print('Daily aggregation comleted')
