import time
import statistics as stats
from concurrent.futures import ThreadPoolExecutor
import psycopg2
from psycopg2.errors import SerializationFailure

# ================== CONFIG ==================
DB_NAME = "defaultdb"
TABLE_NAME = "user_review"
TARGET_USER_ID = "AGBFYI2DDIKXC5Y4FARTYDTQBMFQ"

VIEWER_COUNT_LEVELS = [10, 50, 100, 500, 1000]
BUYER_COUNT_LEVELS  = [10, 50, 100, 500, 1000]

OPS_PER_USER = 40
REPEATS_PER_LEVEL = 5
VIEW_LIMIT = 200

# No password since we create users without passwords
USER_PASSWORD = None
# ============================================

def admin_conn():
    return psycopg2.connect(
        dbname=DB_NAME,
        user="root",
        host="127.0.0.1",
        port=26257,
        sslmode='disable'
    )

def app_conn(username, password=None):
    conn_params = {
        "dbname": DB_NAME,
        "user": username,
        "host": "127.0.0.1",
        "port": 26257,
        "sslmode": "disable",
    }
    if password is not None:
        conn_params["password"] = password
    return psycopg2.connect(**conn_params)

import time
import logging
import psycopg2
from psycopg2.errors import SerializationFailure

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def retry_on_serialization_failure(retries=5, delay=0.1):
    def decorator(func):
        def wrapper(conn, cur, *args, **kwargs):
            attempt = 0
            while True:
                try:
                    logging.info("Starting transaction attempt %d", attempt + 1)
                    result = func(conn, cur, *args, **kwargs)
                    logging.info("Transaction attempt %d succeeded", attempt + 1)
                    return result
                except SerializationFailure as e:
                    if attempt >= retries:
                        logging.error("Retries exhausted after %d attempts. Last error: %s", attempt + 1, e)
                        raise
                    attempt += 1
                    logging.warning("Serialization failure on attempt %d, retrying... Error: %s", attempt, e)
                    conn.rollback()
                    sleep_time = delay * (2 ** attempt)
                    logging.info("Sleeping %.2f seconds before retrying", sleep_time)
                    time.sleep(sleep_time)
        return wrapper
    return decorator

@retry_on_serialization_failure()
def create_users_and_roles(conn, cur):
    logging.info("Creating roles and granting permissions")
    cur.execute("CREATE ROLE IF NOT EXISTS viewer;")
    cur.execute(f"GRANT SELECT ON TABLE {TABLE_NAME} TO viewer;")

    cur.execute("CREATE ROLE IF NOT EXISTS buyer;")
    cur.execute(f"GRANT SELECT, UPDATE, DELETE ON TABLE {TABLE_NAME} TO buyer;")

    required_viewers = max(VIEWER_COUNT_LEVELS)
    required_buyers = max(BUYER_COUNT_LEVELS)

    logging.info("Creating %d viewer users", required_viewers)
    for i in range(required_viewers):
        username = f"viewer_{i}"
        cur.execute(f"CREATE USER IF NOT EXISTS {username};")
        cur.execute(f"GRANT viewer TO {username};")
        logging.info("Created and granted permissions to %s", username)
        time.sleep(0.05)

    logging.info("Creating %d buyer users", required_buyers)
    for i in range(required_buyers):
        username = f"buyer_{i}"
        cur.execute(f"CREATE USER IF NOT EXISTS {username};")
        cur.execute(f"GRANT buyer TO {username};")
        logging.info("Created and granted permissions to %s", username)
        time.sleep(0.05)

    conn.commit()
    logging.info("Committed all changes successfully")


def viewer_worker(credential_idx):
    conn = app_conn(f"viewer_{credential_idx}")
    cur = conn.cursor()
    lat = []
    for _ in range(OPS_PER_USER):
        t0 = time.perf_counter()
        cur.execute(f"SELECT * FROM {TABLE_NAME} WHERE rating = 5 LIMIT %s;", (VIEW_LIMIT,))
        cur.fetchall()
        lat.append(time.perf_counter() - t0)
    conn.close()
    return lat

def buyer_worker(credential_idx):
    conn = app_conn(f"buyer_{credential_idx}")
    cur = conn.cursor()
    lat = []
    for _ in range(OPS_PER_USER):
        # Toggle field to ensure real writes
        cur.execute(f"UPDATE {TABLE_NAME} SET verified_purchase = TRUE WHERE user_id = %s;", (TARGET_USER_ID,))
        conn.commit()
        t0 = time.perf_counter()
        cur.execute(f"UPDATE {TABLE_NAME} SET verified_purchase = FALSE WHERE user_id = %s;", (TARGET_USER_ID,))
        conn.commit()
        lat.append(time.perf_counter() - t0)
    conn.close()
    return lat

def run_level(role: str, n_users: int):
    worker = viewer_worker if role == "viewer" else buyer_worker

    # warm-up
    worker(0)

    samples = []
    for _ in range(REPEATS_PER_LEVEL):
        with ThreadPoolExecutor(max_workers=n_users) as ex:
            results = list(ex.map(worker, range(n_users)))
        for l in results:
            samples.extend(l)

    samples_sorted = sorted(samples)
    idx = max(0, int(0.95 * len(samples_sorted)) - 1)
    p95 = samples_sorted[idx] if samples_sorted else 0.0
    return p95 * 1000.0  # ms

    

def main():
    ensure_roles_and_users()

    results = []

    # VIEWER benchmark
    for n in VIEWER_COUNT_LEVELS:
        p95_ms = run_level("viewer", n)
        results.append(("Simple", "Low", "Read", n, p95_ms))
        print(f"[viewer] users={n}  p95={p95_ms:.2f} ms")

    # BUYER benchmark
    for n in BUYER_COUNT_LEVELS:
        p95_ms = run_level("buyer", n)
        results.append(("Complex", "High", "Update", n, p95_ms))
        print(f"[buyer ] users={n}  p95={p95_ms:.2f} ms")

    # Pretty table
    header = ("Access Control Type", "Complexity", "Operation", "Number of Users", "Enforcement Time (ms, p95) (CockroachDB)")
    colw = [22, 12, 10, 16, 28]
    print("\n" + " | ".join(h.ljust(w) for h, w in zip(header, colw)))
    print("-" * (sum(colw) + 3 * (len(colw) - 1)))
    for r in results:
        row = (r[0], r[1], r[2], str(r[3]), f"{r[4]:.2f}")
        print(" | ".join(v.ljust(w) for v, w in zip(row, colw)))

if __name__ == "__main__":
    main()
