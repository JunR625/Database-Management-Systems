import time
import psycopg2
import matplotlib.pyplot as plt
import numpy as np
import psutil
import os
from pathlib import Path

# ---------- Config ----------
DBNAME = "defaultdb"
USER = "root"
HOST = "127.0.0.1"
PORT = 26257
TABLE = "user_review"           # Your table name
SAMPLE_SIZES = list(range(10_000, 100_001, 10_000))  # 10k..100k

Path("Images").mkdir(exist_ok=True)

# ---------- Connect to CockroachDB ----------
conn = psycopg2.connect(
    dbname=DBNAME,
    user=USER,
    host=HOST,
    port=PORT,
    sslmode='disable'  # Change to 'require' if you have TLS enabled
)
conn.autocommit = True  # Auto-commit for DDL and DML
cur = conn.cursor()

# ---------- Ensure table exists ----------
cur.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    id SERIAL PRIMARY KEY,
    rating INT,
    title STRING,
    text STRING,
    asin STRING,
    parent_asin STRING,
    user_id STRING,
    timestamp STRING,
    helpful_vote INT,
    verified_purchase BOOL
)
""")

# ---------- Sample document data ----------
BASE_DOC = {
    "rating": 5,
    "title": "cute",
    "text": "very cute",
    "asin": "B09DQ5M2BB",
    "parent_asin": "B09DQ5M2BB",
    "user_id": "AFNT6ZJCYQN3WDIKUSWHJDXNND2Q",
    "timestamp": "12:33:48 AM",
    "helpful_vote": 3,
    "verified_purchase": True,
}

def generate_docs(n):
    # Just return a list of tuples matching table columns except id (auto)
    docs = []
    for _ in range(n):
        docs.append((
            BASE_DOC["rating"],
            BASE_DOC["title"],
            BASE_DOC["text"],
            BASE_DOC["asin"],
            BASE_DOC["parent_asin"],
            BASE_DOC["user_id"],
            BASE_DOC["timestamp"],
            BASE_DOC["helpful_vote"],
            BASE_DOC["verified_purchase"],
        ))
    return docs

# ---------- Process monitor ----------
process = psutil.Process(os.getpid())

cpu_usages = []
mem_usages = []

for size in SAMPLE_SIZES:
    print(f"\n--- Measuring with {size} documents ---")

    # Clear table
    cur.execute(f"TRUNCATE TABLE {TABLE}")

    docs = generate_docs(size)

    start_time = time.time()

    # Bulk insert using executemany
    insert_query = f"""
        INSERT INTO {TABLE} 
        (rating, title, text, asin, parent_asin, user_id, timestamp, helpful_vote, verified_purchase) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.executemany(insert_query, docs)

    # Update all rows to helpful_vote=10
    cur.execute(f"UPDATE {TABLE} SET helpful_vote = 10")

    # Delete all rows
    cur.execute(f"DELETE FROM {TABLE}")

    elapsed = time.time() - start_time

    cpu_percent = process.cpu_percent(interval=0.1)
    mem_mb = process.memory_info().rss / (1024 * 1024)

    cpu_usages.append(cpu_percent)
    mem_usages.append(mem_mb)

    print(f"CPU%: {cpu_percent:.2f}, Memory: {mem_mb:.2f} MB, Elapsed: {elapsed:.4f} s")

# Cleanup: drop table
cur.execute(f"DROP TABLE {TABLE}")

cur.close()
conn.close()

# ---------- Plot CPU usage ----------
plt.figure(figsize=(8, 5))
plt.plot(SAMPLE_SIZES, cpu_usages, marker="o", label="CPU Usage (%)")
plt.xlabel("Number of Documents")
plt.ylabel("CPU Usage (%)")
plt.title("CPU Usage vs Number of Documents")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.savefig("Images/cpu_usage_vs_docs.png", dpi=150)
plt.show()

# ---------- Plot Memory usage ----------
plt.figure(figsize=(8, 5))
plt.plot(SAMPLE_SIZES, mem_usages, marker="o", color="orange", label="Memory Usage (MB)")
plt.xlabel("Number of Documents")
plt.ylabel("Memory Usage (MB)")
plt.title("Memory Usage vs Number of Documents")
plt.grid(True)
plt.legend()
plt.tight_layout()
try:
    plt.savefig("Images/memory_usage_vs_docs.png", dpi=150)
except:
    pass
plt.show()
