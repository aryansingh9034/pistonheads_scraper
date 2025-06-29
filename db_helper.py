# db_helper.py — Updated for single leads table with source tracking
import mysql.connector
from mysql.connector import pooling, Error
import time
from datetime import datetime

# Updated database configuration with remote credentials
DB_CFG = dict(
    host="103.54.182.26",
    port=4036,
    user="eddytools",
    password="V#$2)6E!)*sVC6{!*fi0",
    database="traders_leads",
    auth_plugin="mysql_native_password",
    connect_timeout=30,
    autocommit=False
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
    pool = _pool
except Exception as e:
    print(f"❌ Failed to create database pool: {e}")
    pool = None

def ensure_leads_table_exists():
    """Ensure the leads table exists with all required columns"""
    if not pool:
        print("❌ No database connection available")
        return False
        
    try:
        cn = pool.get_connection()
        cur = cn.cursor()
        
        # Check if leads table exists
        cur.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = 'leads'
        """, (DB_CFG['database'],))
        
        exists = cur.fetchone()[0] > 0
        
        if not exists:
            print(f"📝 Creating leads table...")
            cur.execute("""
                CREATE TABLE leads (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    company_name VARCHAR(200),
                    website_address VARCHAR(255),
                    contact_form_url VARCHAR(255),
                    phone_number VARCHAR(20),
                    email VARCHAR(120),
                    information TEXT,
                    status ENUM('new_contact', 'email_001', 'email_002', 'email_003') DEFAULT 'new_contact',
                    source VARCHAR(50),
                    assigned_user_id INT,
                    address_line_1 VARCHAR(255),
                    address_line_2 VARCHAR(255),
                    town VARCHAR(100),
                    city VARCHAR(100),
                    country VARCHAR(100),
                    postcode VARCHAR(20),
                    first_name VARCHAR(50),
                    last_name VARCHAR(50),
                    contact_number VARCHAR(20),
                    email_address VARCHAR(120),
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    last_updated DATETIME,
                    automation_stage VARCHAR(50),
                    
                    -- Vehicle specific fields
                    listing_url VARCHAR(500) UNIQUE,
                    make VARCHAR(100),
                    model VARCHAR(100),
                    variant VARCHAR(200),
                    year VARCHAR(10),
                    price VARCHAR(20),
                    mileage VARCHAR(20),
                    fuel_type VARCHAR(50),
                    body_type VARCHAR(50),
                    gearbox VARCHAR(50),
                    
                    INDEX idx_source (source),
                    INDEX idx_status (status),
                    INDEX idx_created (created_at),
                    INDEX idx_make_model (make, model),
                    INDEX idx_listing_url (listing_url)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            cn.commit()
            print(f"✅ Leads table created successfully")
        else:
            # Check and add missing columns
            missing_columns = [
                ('listing_url', 'VARCHAR(500) UNIQUE'),
                ('make', 'VARCHAR(100)'),
                ('model', 'VARCHAR(100)'),
                ('variant', 'VARCHAR(200)'),
                ('year', 'VARCHAR(10)'),
                ('price', 'VARCHAR(20)'),
                ('mileage', 'VARCHAR(20)'),
                ('fuel_type', 'VARCHAR(50)'),
                ('body_type', 'VARCHAR(50)'),
                ('gearbox', 'VARCHAR(50)'),
                ('source', 'VARCHAR(50)')
            ]
            
            for col_name, col_def in missing_columns:
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM information_schema.columns 
                    WHERE table_schema = %s 
                    AND table_name = 'leads' 
                    AND column_name = %s
                """, (DB_CFG['database'], col_name))
                
                if cur.fetchone()[0] == 0:
                    print(f"📝 Adding missing column '{col_name}' to leads table...")
                    try:
                        cur.execute(f"ALTER TABLE leads ADD COLUMN {col_name} {col_def}")
                        cn.commit()
                    except Error as e:
                        if 'Duplicate' not in str(e):
                            print(f"⚠️ Error adding column {col_name}: {e}")
        
        cur.close()
        cn.close()
        return True
        
    except Error as e:
        print(f"❌ Error ensuring table exists: {e}")
        return False

def save_to_leads(rows: list[dict], source: str):
    """Save rows to leads table with source tracking"""
    if not rows:
        print("⚠️ No rows to save")
        return
        
    if not pool:
        print("❌ No database connection available")
        return
    
    # Ensure table exists
    if not ensure_leads_table_exists():
        print(f"❌ Failed to ensure leads table exists")
        return
    
    inserted_count = 0
    updated_count = 0
    error_count = 0
    
    try:
        cn = pool.get_connection()
        cur = cn.cursor()
        
        # Prepare SQL
        sql = """
            INSERT INTO leads (
                listing_url, company_name, information, 
                make, model, variant, year, price, mileage,
                fuel_type, body_type, gearbox,
                phone_number, address_line_1, city,
                source, status
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s
            )
            ON DUPLICATE KEY UPDATE
                company_name = VALUES(company_name),
                information = VALUES(information),
                make = VALUES(make),
                model = VALUES(model),
                variant = VALUES(variant),
                year = VALUES(year),
                price = VALUES(price),
                mileage = VALUES(mileage),
                fuel_type = VALUES(fuel_type),
                body_type = VALUES(body_type),
                gearbox = VALUES(gearbox),
                phone_number = VALUES(phone_number),
                address_line_1 = VALUES(address_line_1),
                city = VALUES(city),
                updated_at = NOW()
        """
        
        for i, row in enumerate(rows, 1):
            try:
                # Handle nested structure from scrapers
                vehicle = row.get('vehicle', {})
                dealer = row.get('dealer', {})
                
                # Build information field from vehicle title or make/model
                info = vehicle.get('title', '')
                if not info and vehicle.get('make'):
                    info = f"{vehicle.get('make', '')} {vehicle.get('model', '')} {vehicle.get('variant', '')}".strip()
                
                # Prepare values
                values = (
                    row.get('listing_url', ''),
                    dealer.get('name', ''),  # company_name
                    info,  # information
                    vehicle.get('make', ''),
                    vehicle.get('model', ''),
                    vehicle.get('variant', ''),
                    vehicle.get('year', ''),
                    vehicle.get('price', ''),
                    vehicle.get('mileage', ''),
                    vehicle.get('fuel_type', ''),
                    vehicle.get('body_type', ''),
                    vehicle.get('gearbox', ''),
                    dealer.get('phone', ''),  # phone_number
                    dealer.get('location', ''),  # address_line_1
                    dealer.get('city', ''),  # city
                    source,  # source (cazoo, piston_heads, the_aa)
                    'new_contact'  # status
                )
                
                # Debug print for first few rows
                if i <= 3:
                    print(f"\n📝 Row {i} data:")
                    print(f"   URL: {row.get('listing_url', 'N/A')}")
                    print(f"   Dealer: {dealer.get('name', 'N/A')}")
                    print(f"   Vehicle: {vehicle.get('make', 'N/A')} {vehicle.get('model', 'N/A')}")
                    print(f"   Price: {vehicle.get('price', 'N/A')}")
                
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
                print(f"     Row data: {row}")
                # Continue with next row
        
        # Final commit
        cn.commit()
        
        # Get total count for this source
        cur.execute("SELECT COUNT(*) FROM leads WHERE source = %s", (source,))
        source_count = cur.fetchone()[0]
        
        # Get total count
        cur.execute("SELECT COUNT(*) FROM leads")
        total_count = cur.fetchone()[0]
        
        print(f"\n✅ Database operation complete for {source}:")
        print(f"   - New records inserted: {inserted_count}")
        print(f"   - Records updated: {updated_count}")
        print(f"   - Errors: {error_count}")
        print(f"   - Total {source} records: {source_count}")
        print(f"   - Total records in leads table: {total_count}")
        
        cur.close()
        cn.close()
        
    except Error as e:
        print(f"❌ Database save failed: {e}")
        if 'cn' in locals() and cn.is_connected():
            cn.rollback()
            cn.close()

def get_stats_by_source() -> dict:
    """Get statistics grouped by source"""
    if not pool:
        return {}
        
    try:
        cn = pool.get_connection()
        cur = cn.cursor()
        
        # Get count by source
        cur.execute("""
            SELECT source, COUNT(*) as count 
            FROM leads 
            WHERE source IS NOT NULL 
            GROUP BY source
        """)
        source_counts = dict(cur.fetchall())
        
        # Get total count
        cur.execute("SELECT COUNT(*) FROM leads")
        total = cur.fetchone()[0]
        
        # Get recent records
        cur.execute("""
            SELECT source, COUNT(*) as count 
            FROM leads 
            WHERE created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
            AND source IS NOT NULL
            GROUP BY source
        """)
        recent_counts = dict(cur.fetchall())
        
        cur.close()
        cn.close()
        
        return {
            'total': total,
            'by_source': source_counts,
            'last_24h': recent_counts
        }
        
    except Error as e:
        print(f"❌ Error getting stats: {e}")
        return {}

# Backward compatibility function
def save_rows(table: str, rows: list[dict]):
    """Backward compatible function that routes to save_to_leads"""
    # Determine source from table name
    if 'pistonheads' in table.lower():
        source = 'piston_heads'
    elif 'aa' in table.lower():
        source = 'the_aa'
    else:
        source = 'cazoo'
    
    save_to_leads(rows, source)