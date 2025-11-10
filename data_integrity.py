import psycopg2
from psycopg2 import sql
import threading
import time

DBNAME = "defaultdb"
HOST = "127.0.0.1"
PORT = 26257

# Sample users you created (in insecure mode, no password needed)
users = [
    "viewer_user_1",
    "viewer_user_2",
    "buyer_user_1",
    "buyer_user_2",
]

def run_viewer_workload(user):
    try:
        conn = psycopg2.connect(
            dbname=DBNAME,
            user=user,
            host=HOST,
            port=PORT,
            sslmode='disable'
        )
        cur = conn.cursor()
        # Simple read-only query
        for _ in range(5):
            cur.execute("SELECT * FROM user_review WHERE rating = 5 LIMIT 10;")
            rows = cur.fetchall()
            print(f"[{user}] Retrieved {len(rows)} rows")
            time.sleep(0.2)
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[{user}] ERROR: {e}")

def run_buyer_workload(user):
    try:
        conn = psycopg2.connect(
            dbname=DBNAME,
            user=user,
            host=HOST,
            port=PORT,
            sslmode='disable'
        )
        cur = conn.cursor()
        target_user_id = 'AGBFYI2DDIKXC5Y4FARTYDTQBMFQ'
        for _ in range(5):
            # Reset verified_purchase = true before update for testing
            cur.execute(
                "UPDATE user_review SET verified_purchase = true WHERE user_id = %s;",
                (target_user_id,)
            )
            conn.commit()
            # Update to false (timed operation)
            cur.execute(
                "UPDATE user_review SET verified_purchase = false WHERE user_id = %s;",
                (target_user_id,)
            )
            conn.commit()
            print(f"[{user}] Performed update cycle")
            time.sleep(0.2)
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[{user}] ERROR: {e}")

def main():
    threads = []

    # Start viewer workload threads
    for user in [u for u in users if u.startswith("viewer_user")]:
        t = threading.Thread(target=run_viewer_workload, args=(user,))
        t.start()
        threads.append(t)

    # Start buyer workload threads
    for user in [u for u in users if u.startswith("buyer_user")]:
        t = threading.Thread(target=run_buyer_workload, args=(user,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

if __name__ == "__main__":
    main()
