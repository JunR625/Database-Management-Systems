import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

DBNAME = "defaultdb"
USER = "root"
HOST = "127.0.0.1"
PORT = 26257
EXCEL_FILE = "data/dtb_100,000.xlsx"
TABLE_NAME = "user_review"

df = pd.read_excel(EXCEL_FILE, engine='openpyxl', nrows=100000)

# Clean / fill missing data
df['rating'] = df['rating'].astype(int).astype(object)
df['helpful_vote'] = df['helpful_vote'].astype(int).astype(object)
df['verified_purchase'] = df['verified_purchase'].astype(bool).astype(object)

for col in ['title', 'text', 'asin', 'parent_asin', 'user_id', 'timestamp']:
    df[col] = df[col].astype(str).astype(object)

records = [tuple(x) for x in df.to_records(index=False)]


conn = psycopg2.connect(
    dbname=DBNAME,
    user=USER,
    host=HOST,
    port=PORT,
    sslmode='disable'
)

cur = conn.cursor()

sql = f"""
INSERT INTO {TABLE_NAME} (
    rating, title, text, asin, parent_asin,
    user_id, timestamp, helpful_vote, verified_purchase
) VALUES %s
"""

execute_values(cur, sql, records)
conn.commit()

cur.close()
conn.close()

print("Inserted first 100,000 rows into user_review.")
