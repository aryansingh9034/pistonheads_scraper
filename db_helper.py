# db_helper.py — Enhanced MySQL pool + generic save_rows() with better error handling
import mysql.connector
from mysql.connector import pooling, Error
import time

DB_CFG = dict(
    host="127.0.0.1",
    port=3306,
    user="root",
    password="Db@2025#ind$",
    database="traders_leads",
    auth_plugin="mysql_native_password"
)

POOL_CFG = dict(
    pool_name="main_pool",
    pool_size=10,
    pool_reset_session=True
)

# Initialize connection pool with retry logic
def create_pool(retries=3):
    """Create connection pool with retry logic"""
    for attempt in range(retries):
        try:
            pool = pooling.MySQLConnectionPool(**DB_CFG, **POOL_CFG)
            print("✅ Database connection pool created successfully")
            return pool
        except Error as e:
            print(f"❌ Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(2)
            else:
                raise
    return None

# Create the pool
try:
    _pool = create_pool()
    pool = _pool  # Expose pool for external use
except Exception as e:
    print(f"❌ Failed to create database pool: {e}")
    pool = None

def ensure_table_exists(table_name: str):
    """Ensure the table exists with the correct structure"""
    if not pool:
        print("❌ No database connection available")
        return False
        
    try:
        cn = pool.get_connection()
        cur = cn.cursor()
        
        # Check if table exists
        cur.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s
        """, (DB_CFG['database'], table_name))
        
        exists = cur.fetchone()[0] > 0
        
        if not exists:
            print(f"📝 Creating table {table_name}...")
            
            # Create table based on name
            if table_name == "raw_pistonheads_db" or table_name == "raw_aa_db":
                cur.execute(f"""
                    CREATE TABLE {table_name} (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        listing_url VARCHAR(500) UNIQUE,
                        title TEXT,
                        make VARCHAR(100),
                        model VARCHAR(100),
                        variant VARCHAR(200),
                        year VARCHAR(10),
                        price VARCHAR(20),
                        mileage VARCHAR(20),
                        fuel_type VARCHAR(50),
                        body_type VARCHAR(50),
                        gearbox VARCHAR(50),
                        dealer_name VARCHAR(200),
                        dealer_phone VARCHAR(50),
                        dealer_location TEXT,
                        dealer_city VARCHAR(100),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_dealer_name (dealer_name),
                        INDEX idx_make_model (make, model),
                        INDEX idx_year (year),
                        INDEX idx_created (created_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)
                cn.commit()
                print(f"✅ Table {table_name} created successfully")
        
        cur.close()
        cn.close()
        return True
        
    except Error as e:
        print(f"❌ Error ensuring table exists: {e}")
        return False

def save_rows(table: str, rows: list[dict]):
    """Save rows with upsert logic and better error handling"""
    if not rows:
        print("⚠️ No rows to save")
        return
        
    if not pool:
        print("❌ No database connection available")
        return
    
    # Ensure table exists
    if not ensure_table_exists(table):
        print(f"❌ Failed to ensure table {table} exists")
        return
    
    inserted_count = 0
    updated_count = 0
    error_count = 0
    
    try:
        cn = pool.get_connection()
        cur = cn.cursor()
        
        # Get column names from first row
        cols = list(rows[0].keys())
        col_csv = ",".join(cols)
        ph = ",".join(["%s"] * len(cols))
        
        # Create update clause for duplicate keys
        update_cols = [c for c in cols if c not in ['id', 'listing_url', 'created_at']]
        upd = ",".join([f"{c}=VALUES({c})" for c in update_cols])
        
        sql = f"""
            INSERT INTO {table} ({col_csv}) 
            VALUES ({ph}) 
            ON DUPLICATE KEY UPDATE {upd}
        """
        
        for i, row in enumerate(rows, 1):
            try:
                # Ensure all expected columns have values (use None for missing)
                values = tuple(row.get(c) for c in cols)
                cur.execute(sql, values)
                
                if cur.rowcount == 1:
                    inserted_count += 1
                elif cur.rowcount == 2:  # MySQL returns 2 for updates
                    updated_count += 1
                    
                # Commit every 50 rows
                if i % 50 == 0:
                    cn.commit()
                    print(f"  💾 Saved {i}/{len(rows)} rows...")
                    
            except Error as e:
                error_count += 1
                print(f"  ⚠️ Error on row {i}: {e}")
                # Continue with next row
        
        # Final commit
        cn.commit()
        
        # Get total count
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        total_count = cur.fetchone()[0]
        
        print(f"\n✅ Database operation complete for {table}:")
        print(f"   - New records inserted: {inserted_count}")
        print(f"   - Records updated: {updated_count}")
        print(f"   - Errors: {error_count}")
        print(f"   - Total records in table: {total_count}")
        
        cur.close()
        cn.close()
        
    except Error as e:
        print(f"❌ Database save failed: {e}")
        if 'cn' in locals() and cn.is_connected():
            cn.rollback()
            cn.close()

def get_stats(table: str) -> dict:
    """Get statistics for a table"""
    if not pool:
        return {}
        
    try:
        cn = pool.get_connection()
        cur = cn.cursor()
        
        # Get total count
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        total = cur.fetchone()[0]
        
        # Get count by make
        cur.execute(f"""
            SELECT make, COUNT(*) as count 
            FROM {table} 
            WHERE make IS NOT NULL 
            GROUP BY make 
            ORDER BY count DESC 
            LIMIT 10
        """)
        top_makes = cur.fetchall()
        
        # Get price range
        cur.execute(f"""
            SELECT 
                MIN(CAST(REPLACE(REPLACE(price, '£', ''), ',', '') AS UNSIGNED)) as min_price,
                MAX(CAST(REPLACE(REPLACE(price, '£', ''), ',', '') AS UNSIGNED)) as max_price
            FROM {table}
            WHERE price IS NOT NULL AND price != ''
        """)
        price_range = cur.fetchone()
        
        cur.close()
        cn.close()
        
        return {
            'total': total,
            'top_makes': top_makes,
            'price_range': price_range
        }
        
    except Error as e:
        print(f"❌ Error getting stats: {e}")
        return {}