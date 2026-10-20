import clickhouse_connect
import redis
from .config import settings

# Redis connection
redis_client = redis.from_url(settings.redis_url, decode_responses=True)

# ClickHouse connection
def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_db
    )

def init_dbs():
    # Attempt to ping to verify connection
    try:
        redis_client.ping()
        print("[+] Redis connected successfully.")
    except Exception as e:
        print(f"[-] Redis connection failed: {e}")

    try:
        ch = get_clickhouse_client()
        ch.ping()
        print("[+] ClickHouse connected successfully.")
    except Exception as e:
        print(f"[-] ClickHouse connection failed: {e}")
