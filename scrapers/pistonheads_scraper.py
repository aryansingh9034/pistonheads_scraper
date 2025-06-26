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
# 🔄 CHANGED: Updated table structure to include variant and body_type
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
        
        # 🔄 CHANGED: Always recreate table to ensure new fields exist
        if table_exists:
            print("📝 Dropping old table to add new fields...")
            cursor.execute("DROP TABLE raw_pistonheads_db")
        
        print("📝 Creating table raw_pistonheads_db with all vehicle fields...")
        cursor.execute("""
            CREATE TABLE raw_pistonheads_db (
                id INT AUTO_INCREMENT PRIMARY KEY,
                listing_url VARCHAR(500) UNIQUE,
                title TEXT,
                make VARCHAR(100),
                model VARCHAR(100),
                variant VARCHAR(200),          -- 🆕 NEW FIELD
                year VARCHAR(10),
                price VARCHAR(20),
                mileage VARCHAR(20),
                fuel_type VARCHAR(50),
                body_type VARCHAR(50),         -- 🆕 NEW FIELD
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
        conn.commit()
        print("✅ Table created successfully with all vehicle fields")
        
        cursor.close()
        return True
        
    except Error as e:
        print(f"❌ Database verification failed: {e}")
        return False
    finally:
        if conn.is_connected():
            conn.close()

# 🔄 CHANGED: Updated to save variant and body_type
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
                # 🔄 CHANGED: Added variant and body_type to INSERT
                cursor.execute("""
                    INSERT INTO raw_pistonheads_db (
                        listing_url, title, make, model, variant, year, price, mileage,
                        fuel_type, body_type, gearbox, dealer_name, dealer_phone, 
                        dealer_location, dealer_city
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        title = VALUES(title),
                        price = VALUES(price),
                        mileage = VALUES(mileage),
                        variant = VALUES(variant),
                        body_type = VALUES(body_type)
                """, (
                    r.get('listing_url'),
                    vehicle.get('title'),
                    vehicle.get('make'),
                    vehicle.get('model'),
                    vehicle.get('variant'),      # 🆕 NEW
                    vehicle.get('year'),
                    vehicle.get('price'),
                    vehicle.get('mileage'),
                    vehicle.get('fuel_type'),
                    vehicle.get('body_type'),     # 🆕 NEW
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

# 🔄 CHANGED: Updated CSV fields
def save_to_csv(results: List[Dict], append=True):
    """Save results to CSV file"""
    mode = 'a' if append else 'w'
    file_exists = append and csv.Path(LOCAL_CSV_FILE).exists() if hasattr(csv, 'Path') else False
    
    with open(LOCAL_CSV_FILE, mode, newline='', encoding='utf-8') as f:
        if results:
            # 🔄 CHANGED: Added variant and body_type to fieldnames
            fieldnames = [
                'timestamp', 'listing_url', 'title', 'make', 'model', 'variant', 'year', 
                'price', 'mileage', 'fuel_type', 'body_type', 'gearbox', 'dealer_name', 
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
                    'variant': vehicle.get('variant'),      # 🆕 NEW
                    'year': vehicle.get('year'),
                    'price': vehicle.get('price'),
                    'mileage': vehicle.get('mileage'),
                    'fuel_type': vehicle.get('fuel_type'),
                    'body_type': vehicle.get('body_type'),  # 🆕 NEW
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

# --- WEB SCRAPING FUNCTIONS ---
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

# 🔄 COMPLETELY REWRITTEN: Enhanced extraction for all vehicle details
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

        # === 🆕 ENHANCED VEHICLE DETAILS EXTRACTION ===
        
        # 1. Extract title and parse make/model/variant
        title_elem = soup.find('h1')
        if title_elem:
            title = title_elem.get_text(strip=True)
            data['vehicle']['title'] = title
            
            # Extract year first
            year_match = re.search(r"\b(19|20)\d{2}\b", title)
            if year_match:
                data['vehicle']['year'] = year_match.group()
                # Remove year from title for easier parsing
                title_clean = title.replace(year_match.group(), '').strip()
            else:
                title_clean = title
            
            # Parse make, model, variant
            # Common pattern: Make Model Variant (e.g., "Ford Focus Titanium")
            words = title_clean.split()
            if len(words) >= 1:
                data['vehicle']['make'] = words[0]
            if len(words) >= 2:
                data['vehicle']['model'] = words[1]
            if len(words) >= 3:
                # Everything after model is variant
                data['vehicle']['variant'] = ' '.join(words[2:])

        # 2. Extract price
        price_elem = soup.find(string=re.compile(r'£[\d,]+'))
        if price_elem:
            price_match = re.search(r'£([\d,]+)', str(price_elem))
            if price_match:
                data['vehicle']['price'] = f"£{price_match.group(1)}"

        # 3. 🆕 Extract detailed specifications
        # Look for specification lists/sections
        spec_sections = soup.find_all(['ul', 'div'], class_=re.compile('specs|details|features|key-info'))
        
        # Also check all list items for specs
        all_li_items = soup.find_all('li')
        
        # Combine all text for searching
        all_spec_text = []
        for section in spec_sections:
            all_spec_text.append(section.get_text(' ', strip=True))
        for li in all_li_items:
            all_spec_text.append(li.get_text(strip=True))
        
        combined_text = ' '.join(all_spec_text).lower()
        
        # 🆕 Extract mileage with multiple patterns
        mileage_patterns = [
            r'([\d,]+)\s*miles',
            r'mileage[:\s]*([\d,]+)',
            r'([\d,]+)\s*mi\b'
        ]
        for pattern in mileage_patterns:
            match = re.search(pattern, combined_text)
            if match:
                data['vehicle']['mileage'] = match.group(1).replace(',', '')
                break
        
        # 🆕 Extract fuel type
        fuel_types = ['petrol', 'diesel', 'electric', 'hybrid', 'plug-in hybrid', 'lpg']
        for fuel in fuel_types:
            if fuel in combined_text:
                data['vehicle']['fuel_type'] = fuel.title()
                break
        
        # 🆕 Extract transmission/gearbox
        trans_patterns = [
            r'(manual|automatic|semi-automatic|cvt|dsg)',
            r'transmission[:\s]*(manual|automatic)',
            r'gearbox[:\s]*(manual|automatic)'
        ]
        for pattern in trans_patterns:
            match = re.search(pattern, combined_text, re.I)
            if match:
                data['vehicle']['gearbox'] = match.group(1).capitalize()
                break
        
        # 🆕 Extract body type
        body_types = ['hatchback', 'saloon', 'estate', 'suv', 'coupe', 'convertible', 
                     'mpv', 'pickup', 'van', '4x4', 'sports', 'cabriolet']
        for body in body_types:
            if body in combined_text:
                data['vehicle']['body_type'] = body.title()
                break
        
        # 🆕 Look for specs in table format
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True).lower()
                    value = cells[1].get_text(strip=True)
                    
                    if 'year' in label and not data['vehicle'].get('year'):
                        year_match = re.search(r'(19|20)\d{2}', value)
                        if year_match:
                            data['vehicle']['year'] = year_match.group()
                    elif 'mileage' in label and not data['vehicle'].get('mileage'):
                        data['vehicle']['mileage'] = value.replace(',', '').replace(' miles', '')
                    elif 'fuel' in label and not data['vehicle'].get('fuel_type'):
                        data['vehicle']['fuel_type'] = value
                    elif 'transmission' in label or 'gearbox' in label:
                        data['vehicle']['gearbox'] = value
                    elif 'body' in label and not data['vehicle'].get('body_type'):
                        data['vehicle']['body_type'] = value

        # === 🆕 ENHANCED DEALER EXTRACTION ===
        
        # Method 1: Look for dealer name in various locations
        dealer_selectors = [
            ('h3', re.compile('heading_root.*heading_h5')),
            ('h2', re.compile('dealer|seller')),
            ('div', re.compile('SellerDetails_tradeSellerWrapper')),
            ('div', re.compile('dealer-info|seller-info')),
            ('div', {'class': re.compile('seller.*name')}),
            ('span', {'class': re.compile('dealer.*name')})
        ]
        
        dealer_name = None
        for tag, selector in dealer_selectors:
            if isinstance(selector, dict):
                elem = soup.find(tag, selector)
            else:
                elem = soup.find(tag, class_=selector)
            
            if elem:
                # Look for h3 within the element
                h3 = elem.find('h3') if tag != 'h3' else elem
                if h3:
                    text = h3.get_text(strip=True)
                    if text and not text.startswith('About') and len(text) > 2:
                        dealer_name = text
                        break
                else:
                    # Try to get text directly
                    text = elem.get_text(strip=True)
                    if text and ('cars' in text.lower() or 'motors' in text.lower()):
                        dealer_name = text
                        break
        
        if dealer_name:
            data['dealer']['name'] = dealer_name
        
        # 🆕 Method 2: Look for dealer in "About the seller" section
        if not data['dealer'].get('name'):
            about_seller = soup.find(string=re.compile('About the seller', re.I))
            if about_seller:
                parent = about_seller.find_parent(['div', 'section'])
                if parent:
                    # Look for the next h3 or strong tag
                    for tag in ['h3', 'h2', 'strong', 'b']:
                        name_elem = parent.find(tag)
                        if name_elem:
                            text = name_elem.get_text(strip=True)
                            if text and not text.startswith('About'):
                                data['dealer']['name'] = text
                                break

        # Extract phone
        tel_elem = soup.find('a', href=re.compile(r'tel:'))
        if tel_elem:
            phone = tel_elem.get('href', '').replace('tel:', '')
            data['dealer']['phone'] = phone

        # Extract location
        location_elem = soup.find(string=re.compile(r'\w+,\s*United Kingdom'))
        if location_elem:
            location = location_elem.strip()
            data['dealer']['location'] = location
            
            city_match = re.match(r'([^,]+),', location)
            if city_match:
                data['dealer']['city'] = city_match.group(1)

        # 🆕 Print detailed results for debugging
        print(f"  ✅ Found - Make: {data['vehicle'].get('make')}, "
              f"Model: {data['vehicle'].get('model')}, "
              f"Variant: {data['vehicle'].get('variant')}, "
              f"Year: {data['vehicle'].get('year')}, "
              f"Price: {data['vehicle'].get('price')}, "
              f"Mileage: {data['vehicle'].get('mileage')}, "
              f"Fuel: {data['vehicle'].get('fuel_type')}, "
              f"Body: {data['vehicle'].get('body_type')}, "
              f"Gearbox: {data['vehicle'].get('gearbox')}, "
              f"Dealer: {data['dealer'].get('name')}")

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