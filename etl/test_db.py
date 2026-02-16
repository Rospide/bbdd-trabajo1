from etl.db import get_conn

conn = get_conn()
cur = conn.cursor()
cur.execute("SELECT DATABASE();")
print("DB:", cur.fetchone())

cur.execute("SHOW TABLES;")
print("Tablas:", cur.fetchall())

conn.close()