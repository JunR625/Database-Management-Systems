import psycopg2

conn = psycopg2.connect(
    dbname="defaultdb",
    user="root",
    # password="password",
    host="127.0.0.1",
    port=26257,
    sslmode='disable'
)

cur = conn.cursor()

cur.execute("CREATE USER IF NOT EXISTS viewer_user_1")
cur.execute("GRANT SELECT ON TABLE user_review TO viewer_user_1")

conn.commit()
cur.close()
conn.close()
