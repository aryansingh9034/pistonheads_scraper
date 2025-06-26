# db_helper.py – shared MySQL pool + generic save_rows()
from mysql.connector import pooling

DB_CFG = dict(
    host="127.0.0.1", port=3306,
    user="root", password="Db@2025#ind$",
    database="traders_leads", auth_plugin="mysql_native_password"
)
POOL_CFG = dict(pool_name="main_pool", pool_size=10, pool_reset_session=True)
_pool = pooling.MySQLConnectionPool(**DB_CFG, **POOL_CFG)

def save_rows(table: str, rows: list[dict]):
    if not rows:
        return
    cols = rows[0].keys()
    col_csv = ",".join(cols)
    ph      = ",".join(["%s"] * len(cols))
    upd     = ",".join([f"{c}=VALUES({c})" for c in cols])
    sql = f"INSERT INTO {table} ({col_csv}) VALUES ({ph}) ON DUPLICATE KEY UPDATE {upd}"
    cn = _pool.get_connection(); cur = cn.cursor()
    for r in rows:
        cur.execute(sql, tuple(r[c] for c in cols))
    cn.commit(); cur.close(); cn.close()
