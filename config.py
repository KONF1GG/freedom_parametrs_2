from dotenv import load_dotenv
import os
load_dotenv()

CLICKHOUSE_HOST=os.getenv('CLICKHOUSE_HOST')
CLICKHOUSE_PORT=os.getenv('CLICKHOUSE_PORT')
CLICKHOUSE_USER=os.getenv('CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD=os.getenv('CLICKHOUSE_PASSWORD')