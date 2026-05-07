import os
import clickhouse_connect

# ✅ FORCE LOAD .env HERE
def load_env():
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(env_path):
        print("⚠ .env file not found")
        return

    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ[k.strip()] = v.strip()

load_env()

_client = None

def get_db_client():
    global _client
    if _client is not None:
        return _client

    hosts = [h.strip() for h in os.getenv("CLICKHOUSE_HOSTS", "127.0.0.1").split(",")]
    port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    user = os.getenv("CLICKHOUSE_USER")
    password = os.getenv("CLICKHOUSE_PASSWORD")
    database = os.getenv("CLICKHOUSE_DB", "IOB1")

    print("DEBUG ENV:", hosts, user, password, database)

    for host in hosts:
        try:
            _client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=user,
                password=password,
                database=database
            )
            print(f"✅ Connected to ClickHouse: {host}")
            return _client
        except Exception as e:
            print(f"❌ Failed {host}: {e}")

    raise Exception("ClickHouse connection failed")
