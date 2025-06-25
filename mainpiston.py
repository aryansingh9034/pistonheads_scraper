import asyncio
import json
import re
import csv
from typing import List, Dict, Optional, Tuple
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup
import mysql.connector
from mysql.connector import Error, pooling
from urllib.parse import urlparse, parse_qs, urlencode
from datetime import datetime
import atexit

# --- CONFIG ---
DB_CONFIG = {
    'host': '127.0.0.1',            # ← use localhost when running on the same EC2 box
    'port': 3306,
    'user': 'root',
    'password': 'Db@2025#ind$',     # or your chosen DB password
    'database': 'traders_leads',
    'auth_plugin': 'mysql_native_password',
    'autocommit': False,            # Explicit commit control
    'raise_on_warnings': True
}


# Connection pool configuration
POOL_CONFIG = {
    'pool_name': 'pistonheads_pool',
    'pool_size': 5,
    'pool_reset_session': True
}

PROGRESS_FILE = "scrape_progress.json"
LOCAL_CSV_FILE = "pistonheads_backup.csv"

# Global connection pool
connection_pool = None

# --- CONNECTION POOL MANAGEMENT ---
def init_connection_pool():
    """Initialize connection pool"""
    global connection_pool
    try:
        print("🔌 Initializing connection pool...")
        pool_config = {**DB_CONFIG, **POOL_CONFIG}
        connection_pool = mysql.connector.pooling.MySQLConnectionPool(**pool_config)
        print("✅ Connection pool initialized")
        return True
    except Error as e:
        print(f"❌ Failed to create connection pool: {e}")
        return False

def get_connection_from_pool():
    """Get a connection from the pool"""
    global connection_pool
    if connection_pool:
        try:
            conn = connection_pool.get_connection()
            if conn.is_connected():
                return conn
        except Error as e:
            print(f"❌ Error getting connection from pool: {e}")
    return None

def close_all_connections():
    """Close all connections in the pool"""
    global connection_pool
    if connection_pool:
        try:
            # Get all connections and close them
            for _ in range(connection_pool._cnx_queue.qsize()):
                try:
                    conn = connection_pool.get_connection()
                    conn.close()
                except:
                    pass
            print("✅ All connections closed")
        except Exception as e:
            print(f"⚠️ Error closing connections: {e}")

# Register cleanup on exit
atexit.register(close_all_connections)

# --- DATABASE OPERATIONS WITH VERIFICATION ---
def verify_database_connection():
    """Verify database is accessible and table exists"""
    conn = get_connection_from_pool()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Check if we can query the database
        cursor.execute("SELECT DATABASE()")
        db_name = cursor.fetchone()[0]
        print(f"✅ Connected to database: {db_name}")
        
        # Check if table exists
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_name = 'raw_pistonheads_db'
        """, (db_name,))
        
        table_exists = cursor.fetchone()[0] > 0
        
        if not table_exists:
            print("📝 Creating table raw_pistonheads_db...")
            cursor.execute("""
                CREATE TABLE raw_pistonheads_db (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    listing_url VARCHAR(500) UNIQUE,
                    title TEXT,
                    make VARCHAR(100),
                    model VARCHAR(100),
                    year VARCHAR(10),
                    price VARCHAR(20),
                    mileage VARCHAR(20),
                    fuel_type VARCHAR(50),
                    gearbox VARCHAR(50),
                    dealer_name VARCHAR(200),
                    dealer_phone VARCHAR(50),
                    dealer_location TEXT,
                    dealer_city VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_dealer_name (dealer_name),
                    INDEX idx_created (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            conn.commit()
            print("✅ Table created successfully")
        else:
            # Get current row count
            cursor.execute("SELECT COUNT(*) FROM raw_pistonheads_db")
            row_count = cursor.fetchone()[0]
            print(f"✅ Table exists with {row_count} rows")
        
        cursor.close()
        return True
        
    except Error as e:
        print(f"❌ Database verification failed: {e}")
        return False
    finally:
        if conn.is_connected():
            conn.close()

def save_to_database(results: List[Dict]) -> bool:
    """Save results to database with verification"""
    if not results:
        return False
    
    conn = get_connection_from_pool()
    if not conn:
        print("❌ No database connection available")
        return False
    
    inserted_count = 0
    duplicate_count = 0
    
    try:
        cursor = conn.cursor()
        
        for r in results:
            vehicle = r.get("vehicle", {})
            dealer = r.get("dealer", {})
            
            try:
                cursor.execute("""
                    INSERT INTO raw_pistonheads_db (
                        listing_url, title, make, model, year, price, mileage,
                        fuel_type, gearbox, dealer_name, dealer_phone, 
                        dealer_location, dealer_city
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        title = VALUES(title),
                        price = VALUES(price),
                        mileage = VALUES(mileage)
                """, (
                    r.get('listing_url'),
                    vehicle.get('title'),
                    vehicle.get('make'),
                    vehicle.get('model'),
                    vehicle.get('year'),
                    vehicle.get('price'),
                    vehicle.get('mileage'),
                    vehicle.get('fuel_type'),
                    vehicle.get('gearbox'),
                    dealer.get('name'),
                    dealer.get('phone'),
                    dealer.get('location'),
                    dealer.get('city'),
                ))
                
                if cursor.rowcount == 1:
                    inserted_count += 1
                else:
                    duplicate_count += 1
                    
            except Error as e:
                print(f"⚠️ Error inserting {r.get('listing_url')}: {e}")
        
        # Commit all changes
        conn.commit()
        
        # Verify insertion
        cursor.execute("SELECT COUNT(*) FROM raw_pistonheads_db")
        total_rows = cursor.fetchone()[0]
        
        print(f"✅ Database operation complete:")
        print(f"   - New records inserted: {inserted_count}")
        print(f"   - Duplicates updated: {duplicate_count}")
        print(f"   - Total rows in table: {total_rows}")
        
        cursor.close()
        return True
        
    except Error as e:
        print(f"❌ Database save failed: {e}")
        if conn.is_connected():
            conn.rollback()
        return False
    finally:
        if conn.is_connected():
            conn.close()

# --- CSV FALLBACK ---
def save_to_csv(results: List[Dict], append=True):
    """Save results to CSV file"""
    mode = 'a' if append else 'w'
    file_exists = append and csv.Path(LOCAL_CSV_FILE).exists() if hasattr(csv, 'Path') else False
    
    with open(LOCAL_CSV_FILE, mode, newline='', encoding='utf-8') as f:
        if results:
            fieldnames = [
                'timestamp', 'listing_url', 'title', 'make', 'model', 'year', 
                'price', 'mileage', 'fuel_type', 'gearbox', 'dealer_name', 
                'dealer_phone', 'dealer_location', 'dealer_city'
            ]
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            if not file_exists or mode == 'w':
                writer.writeheader()
            
            for r in results:
                vehicle = r.get("vehicle", {})
                dealer = r.get("dealer", {})
                row = {
                    'timestamp': datetime.now().isoformat(),
                    'listing_url': r.get('listing_url'),
                    'title': vehicle.get('title'),
                    'make': vehicle.get('make'),
                    'model': vehicle.get('model'),
                    'year': vehicle.get('year'),
                    'price': vehicle.get('price'),
                    'mileage': vehicle.get('mileage'),
                    'fuel_type': vehicle.get('fuel_type'),
                    'gearbox': vehicle.get('gearbox'),
                    'dealer_name': dealer.get('name'),
                    'dealer_phone': dealer.get('phone'),
                    'dealer_location': dealer.get('location'),
                    'dealer_city': dealer.get('city')
                }
                writer.writerow(row)
    
    print(f"💾 Saved {len(results)} records to {LOCAL_CSV_FILE}")

# --- MAIN SAVE FUNCTION ---
def save_results(results: List[Dict]):
    """Save results with database priority and CSV backup"""
    if not results:
        print("⚠️ No results to save")
        return
    
    # Try database first
    db_success = save_to_database(results)
    
    # Always save to CSV as backup
    save_to_csv(results)
    
    if not db_success:
        print("⚠️ Database save failed, but data saved to CSV")

# --- PROGRESS TRACKING ---
def load_progress() -> Dict:
    try:
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"start_page": 1, "last_successful_page": 0, "total_scraped": 0}

def save_progress(progress: Dict):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)

# --- WEB SCRAPING FUNCTIONS (unchanged) ---
async def extract_listings_and_next_page(search_url: str) -> Tuple[List[str], Optional[str]]:
    print(f"\n🔍 Processing page: {search_url}")
    
    async with AsyncWebCrawler(verbose=False, headless=True) as crawler:
        result = await crawler.arun(
            url=search_url,
            timeout=30000,
            js_code="""
            await new Promise(resolve => setTimeout(resolve, 3000));
            window.scrollTo(0, document.body.scrollHeight);
            await new Promise(resolve => setTimeout(resolve, 2000));
            """
        )
        
        if not result.success:
            print("❌ Failed to fetch page")
            return [], None

        soup = BeautifulSoup(result.html, 'html.parser')
        listing_urls = []
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            if '/buy/listing/' in href and 'thumb' not in href:
                full_url = href if href.startswith('http') else f"https://www.pistonheads.com{href}"
                clean_url = full_url.split('?')[0]
                if clean_url not in listing_urls:
                    listing_urls.append(clean_url)
        
        print(f"✅ Found {len(listing_urls)} listings on this page")
        
        parsed = urlparse(search_url)
        params = parse_qs(parsed.query)
        current_page = int(params.get('page', ['1'])[0])
        params['page'] = [str(current_page + 1)]
        next_page_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{urlencode(params, doseq=True)}"
        
        return listing_urls, next_page_url

async def extract_listing_details(listing_url: str) -> Dict:
    print(f"  🚗 Processing: {listing_url}")
    
    async with AsyncWebCrawler(verbose=False, headless=True) as crawler:
        result = await crawler.arun(
            url=listing_url,
            timeout=30000,
            js_code="""
            await new Promise(resolve => setTimeout(resolve, 3000));
            window.scrollTo(0, document.body.scrollHeight);
            await new Promise(resolve => setTimeout(resolve, 2000));
            """
        )
        
        if not result.success:
            print(f"  ❌ Failed to fetch listing")
            return {}

        soup = BeautifulSoup(result.html, 'html.parser')
        data = {"listing_url": listing_url, "vehicle": {}, "dealer": {}}

        # Extract vehicle details
        title_elem = soup.find('h1')
        if title_elem:
            title = title_elem.get_text(strip=True)
            data['vehicle']['title'] = title
            
            year_match = re.search(r"\b(19|20)\d{2}\b", title)
            if year_match:
                data['vehicle']['year'] = year_match.group()
            
            words = title.split()
            if len(words) >= 2:
                data['vehicle']['make'] = words[0] if not words[0].isdigit() else words[1]
                data['vehicle']['model'] = words[1] if not words[0].isdigit() else words[2]

        price_elem = soup.find(string=re.compile(r'£[\d,]+'))
        if price_elem:
            price_match = re.search(r'£([\d,]+)', str(price_elem))
            if price_match:
                data['vehicle']['price'] = f"£{price_match.group(1)}"

        for li in soup.find_all('li'):
            text = li.get_text(strip=True).lower()
            
            if 'miles' in text:
                miles_match = re.search(r'([\d,]+)\s*miles', text)
                if miles_match:
                    data['vehicle']['mileage'] = miles_match.group(1)
            
            for fuel in ['petrol', 'diesel', 'electric', 'hybrid']:
                if fuel in text:
                    data['vehicle']['fuel_type'] = fuel.capitalize()
                    break
            
            for trans in ['manual', 'automatic']:
                if trans in text:
                    data['vehicle']['gearbox'] = trans.capitalize()
                    break

        # Extract dealer details
        dealer_h3 = soup.find('h3', class_=re.compile('heading_root.*heading_h5'))
        if dealer_h3:
            dealer_name = dealer_h3.get_text(strip=True)
            if dealer_name and not dealer_name.startswith('About'):
                data['dealer']['name'] = dealer_name

        tel_elem = soup.find('a', href=re.compile(r'tel:'))
        if tel_elem:
            phone = tel_elem.get('href', '').replace('tel:', '')
            data['dealer']['phone'] = phone

        location_elem = soup.find(string=re.compile(r'\w+,\s*United Kingdom'))
        if location_elem:
            location = location_elem.strip()
            data['dealer']['location'] = location
            
            city_match = re.match(r'([^,]+),', location)
            if city_match:
                data['dealer']['city'] = city_match.group(1)

        return data

# --- MAIN FUNCTION ---
async def main():
    # Initialize connection pool
    if not init_connection_pool():
        print("⚠️ Running without database connection")
    else:
        # Verify database
        if not verify_database_connection():
            print("⚠️ Database verification failed")
    
    # Load progress
    progress = load_progress()
    start_page = progress.get("start_page", 1)
    total_scraped = progress.get("total_scraped", 0)
    
    # Configuration
    batch_size = 3
    max_listings_per_page = 10
    delay_between_listings = 2
    
    end_page = start_page + batch_size - 1
    
    print(f"\n🚀 PistonHeads Scraper - Batch Mode")
    print(f"📄 Pages: {start_page} to {end_page}")
    print(f"📊 Total previously scraped: {total_scraped}")
    print("-" * 50)
    
    all_results = []
    current_page = start_page
    
    while current_page <= end_page:
        url = f"https://www.pistonheads.com/buy/search?price=0&price=2000&seller-type=Trade&page={current_page}"
        
        try:
            listing_urls, next_url = await extract_listings_and_next_page(url)
            
            if not listing_urls:
                print(f"⚠️ No listings found on page {current_page}")
                current_page += 1
                continue
            
            if max_listings_per_page:
                listing_urls = listing_urls[:max_listings_per_page]
            
            page_results = []
            for i, listing_url in enumerate(listing_urls, 1):
                print(f"  [{i}/{len(listing_urls)}]", end="")
                
                try:
                    data = await extract_listing_details(listing_url)
                    if data and data.get("vehicle"):
                        page_results.append(data)
                        print(" ✅")
                    else:
                        print(" ⚠️ No data")
                except Exception as e:
                    print(f" ❌ Error: {e}")
                
                if i < len(listing_urls):
                    await asyncio.sleep(delay_between_listings)
            
            all_results.extend(page_results)
            print(f"📄 Page {current_page} complete: {len(page_results)} listings")
            
            # Save after each page
            if page_results:
                save_results(page_results)
            
            progress["last_successful_page"] = current_page
            progress["total_scraped"] = total_scraped + len(all_results)
            save_progress(progress)
            
        except Exception as e:
            print(f"❌ Error on page {current_page}: {e}")
        
        current_page += 1
        
        if current_page <= end_page:
            await asyncio.sleep(3)
    
    # Final summary
    if all_results:
        print(f"\n✅ Batch complete!")
        print(f"📊 This batch: {len(all_results)} listings")
        print(f"📊 Total scraped: {progress['total_scraped']} listings")
    else:
        print("\n⚠️ No new listings found in this batch")
    
    # Update progress for next run
    progress["start_page"] = end_page + 1
    save_progress(progress)
    print(f"\n➡️ Next run will start at page {progress['start_page']}")
    
    # Close connection pool
    close_all_connections()

if __name__ == "__main__":
    asyncio.run(main())