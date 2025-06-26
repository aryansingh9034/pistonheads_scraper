#!/usr/bin/env python3
# aa_complete_scraper.py - Complete AA Cars scraper with database and pagination

import asyncio
import json
import re
import csv
import sys
from typing import List, Dict, Optional, Tuple
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup
import mysql.connector
from mysql.connector import Error, pooling
from urllib.parse import urlparse, parse_qs, urlencode
from datetime import datetime
import atexit
from pathlib import Path

# --- CONFIG ---
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': 'Db@2025#ind$',
    'database': 'traders_leads',
    'auth_plugin': 'mysql_native_password',
    'autocommit': False,
    'raise_on_warnings': True
}

# Connection pool configuration
POOL_CONFIG = {
    'pool_name': 'aa_pool',
    'pool_size': 5,
    'pool_reset_session': True
}

PROGRESS_FILE = "aa_scrape_progress.json"
LOCAL_CSV_FILE = "aa_backup.csv"

# Global connection pool
connection_pool = None

# JavaScript for page loading - Enhanced for AA Cars
FETCH_JS = """
    // Wait for initial load
    await new Promise(r => setTimeout(r, 3000));
    
    // Scroll to load lazy images and trigger any dynamic content
    window.scrollTo(0, document.body.scrollHeight);
    await new Promise(r => setTimeout(r, 2000));
    
    // Scroll back up
    window.scrollTo(0, 0);
    await new Promise(r => setTimeout(r, 1000));
    
    // Click any "Load More" buttons if they exist
    const loadMoreButtons = document.querySelectorAll('button[class*="load-more"], button[class*="show-more"]');
    for (const button of loadMoreButtons) {
        if (button && !button.disabled) {
            button.click();
            await new Promise(r => setTimeout(r, 2000));
        }
    }
"""

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

# --- DATABASE OPERATIONS ---
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
            AND table_name = 'raw_aa_db'
        """, (db_name,))
        
        table_exists = cursor.fetchone()[0] > 0
        
        if table_exists:
            print("📝 Dropping old table to ensure structure is correct...")
            cursor.execute("DROP TABLE raw_aa_db")
        
        print("📝 Creating table raw_aa_db...")
        cursor.execute("""
            CREATE TABLE raw_aa_db (
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
        conn.commit()
        print("✅ Table created successfully")
        
        cursor.close()
        return True
        
    except Error as e:
        print(f"❌ Database verification failed: {e}")
        return False
    finally:
        if conn.is_connected():
            conn.close()

def save_to_database(results: List[Dict]) -> bool:
    """Save results to database"""
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
                    INSERT INTO raw_aa_db (
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
                    vehicle.get('variant'),
                    vehicle.get('year'),
                    vehicle.get('price'),
                    vehicle.get('mileage'),
                    vehicle.get('fuel_type'),
                    vehicle.get('body_type'),
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
        
        conn.commit()
        
        cursor.execute("SELECT COUNT(*) FROM raw_aa_db")
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
    file_exists = append and Path(LOCAL_CSV_FILE).exists()
    
    with open(LOCAL_CSV_FILE, mode, newline='', encoding='utf-8') as f:
        if results:
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
                    'variant': vehicle.get('variant'),
                    'year': vehicle.get('year'),
                    'price': vehicle.get('price'),
                    'mileage': vehicle.get('mileage'),
                    'fuel_type': vehicle.get('fuel_type'),
                    'body_type': vehicle.get('body_type'),
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
    
    db_success = save_to_database(results)
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
    """Extract listing URLs and next page from AA search page"""
    print(f"\n🔍 Processing page: {search_url}")
    
    async with AsyncWebCrawler(verbose=False, headless=True) as crawler:
        result = await crawler.arun(
            url=search_url,
            timeout=30000,
            js_code=FETCH_JS,
            wait_for_network_idle=2000
        )
        
        if not result.success:
            print("❌ Failed to fetch page")
            return [], None

        soup = BeautifulSoup(result.html, 'html.parser')
        listing_urls = []
        
        # Debug: Save HTML to file for inspection
        debug_file = f"aa_page_debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        with open(debug_file, 'w', encoding='utf-8') as f:
            f.write(result.html)
        print(f"📄 Debug HTML saved to: {debug_file}")
        
        # Look for listing cards - AA uses various class names
        listing_selectors = [
            'div.vehicle-card',
            'div.car-card',
            'article.vehicle-listing',
            'div[class*="vehicle-result"]',
            'div[class*="car-result"]',
            'div[class*="listing-item"]',
            'div.search-result-card',
            'div[data-testid="vehicle-card"]',
            'div[data-testid="search-result"]'
        ]
        
        listing_cards = []
        for selector in listing_selectors:
            cards = soup.select(selector)
            if cards:
                listing_cards.extend(cards)
                print(f"📦 Found {len(cards)} cards with selector: {selector}")
        
        # If no specific cards found, look for any links that might be car details
        if not listing_cards:
            print("🔍 No listing cards found, searching for individual links...")
            all_links = soup.find_all('a', href=True)
            
            for link in all_links:
                href = link['href']
                # Look for patterns that indicate a car detail page
                if any(pattern in href for pattern in ['/cardetails/', '/vehicle/', '/car/', 'vehicleId=', 'carId=']):
                    full_url = f"https://www.theaa.com{href}" if href.startswith('/') else href
                    clean_url = full_url.split('?')[0]
                    if clean_url not in listing_urls and 'theaa.com' in clean_url:
                        listing_urls.append(clean_url)
        else:
            # Extract URLs from listing cards
            for card in listing_cards:
                # Find the main link in the card
                links = card.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    if href and href != '#':
                        full_url = f"https://www.theaa.com{href}" if href.startswith('/') else href
                        clean_url = full_url.split('?')[0]
                        if clean_url not in listing_urls and 'theaa.com' in clean_url:
                            listing_urls.append(clean_url)
                            break  # Take first valid link from each card
        
        # Remove duplicates while preserving order
        listing_urls = list(dict.fromkeys(listing_urls))
        
        print(f"✅ Found {len(listing_urls)} unique listings on this page")
        if listing_urls:
            print("📋 First 5 listings:")
            for i, url in enumerate(listing_urls[:5], 1):
                print(f"   {i}. {url}")
        
        # Find next page
        next_page_url = None
        
        # Method 1: Look for pagination links
        pagination_selectors = [
            'a[aria-label*="Next"]',
            'a.next-page',
            'a[class*="pagination-next"]',
            'button[aria-label*="Next"]',
            'a[rel="next"]',
            '.pagination a'
        ]
        
        for selector in pagination_selectors:
            next_elements = soup.select(selector)
            for elem in next_elements:
                if elem.name == 'a' and elem.get('href'):
                    href = elem['href']
                    if href and href != '#' and 'javascript:' not in href:
                        next_page_url = f"https://www.theaa.com{href}" if href.startswith('/') else href
                        break
            if next_page_url:
                break
        
        # Method 2: Look for page number in URL and increment
        if not next_page_url and listing_urls:  # Only if we found listings
            parsed = urlparse(search_url)
            params = parse_qs(parsed.query)
            
            # Check if there's a page parameter
            if 'page' in params:
                current_page = int(params.get('page', ['1'])[0])
                params['page'] = [str(current_page + 1)]
            elif 'pageNumber' in params:
                current_page = int(params.get('pageNumber', ['1'])[0])
                params['pageNumber'] = [str(current_page + 1)]
            elif 'offset' in params:
                current_offset = int(params.get('offset', ['0'])[0])
                params['offset'] = [str(current_offset + 20)]  # Assuming 20 results per page
            else:
                # No pagination parameter found, add one
                params['page'] = ['2']
            
            next_page_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{urlencode(params, doseq=True)}"
        
        if next_page_url:
            print(f"➡️ Next page: {next_page_url}")
        else:
            print("🏁 No next page found")
        
        return listing_urls, next_page_url

async def extract_aa_listing(url: str) -> Optional[Dict]:
    """Extract vehicle and dealer details from AA listing"""
    print(f"  🚗 Processing: {url}")
    
    async with AsyncWebCrawler(verbose=False, headless=True) as crawler:
        res = await crawler.arun(
            url=url, 
            timeout=45000, 
            js_code=FETCH_JS,
            wait_for_network_idle=2000
        )
    
    if not res.success:
        print(f"  ❌ Fetch failed: {res.error}")
        return None

    soup = BeautifulSoup(res.html, "html.parser")
    out = {"listing_url": url, "vehicle": {}, "dealer": {}}
    v, d = out["vehicle"], out["dealer"]

    # 1. Primary source: utag_data JSON
    if script := soup.find("script", string=re.compile(r"var\s+utag_data")):
        if match := re.search(r"var\s+utag_data\s*=\s*({.*?});", script.string, re.S):
            try:
                jdata = json.loads(match.group(1))
                v_mapping = {
                    "make": "make",
                    "model": "model",
                    "vehicleVariant": "variant",
                    "fuelType": "fuel_type",
                    "bodyType": "body_type",
                    "transmissionType": "gearbox",
                    "mileage": "mileage",
                    "modelYear": "year"
                }
                for src_key, dest_key in v_mapping.items():
                    if value := jdata.get(src_key):
                        v[dest_key] = value
                
                if dealer_name := jdata.get("dealerName"):
                    d["name"] = dealer_name
            except json.JSONDecodeError:
                pass

    # 2. Secondary source: Key facts
    for li in soup.select("li.vd-spec"):
        if (label := li.select_one(".vd-spec-label")) and (value := li.select_one(".vd-spec-value")):
            lab = label.get_text(strip=True).lower()
            val = value.get_text(strip=True)
            if lab == "year" and "year" not in v:
                v["year"] = val
            elif lab == "make" and "make" not in v:
                v["make"] = val
            elif lab == "model" and "model" not in v:
                v["model"] = val
            elif lab == "mileage" and "mileage" not in v:
                v["mileage"] = re.sub(r"[^\d]", "", val)
            elif lab == "fuel type" and "fuel_type" not in v:
                v["fuel_type"] = val
            elif lab == "transmission" and "gearbox" not in v:
                v["gearbox"] = val
            elif lab == "body type" and "body_type" not in v:
                v["body_type"] = val

    # 3. Fallback: Title decomposition
    if h1 := soup.select_one("h1"):
        title = h1.get_text(strip=True)
        v.setdefault("title", title)
        
        # Improved title parsing
        if "year" not in v or "make" not in v or "model" not in v:
            year_match = re.search(r"\b(20\d{2}|19\d{2})\b", title)
            if year_match and "year" not in v:
                v["year"] = year_match.group(0)
            
            # Extract make/model after removing year
            model_text = re.sub(r"\b\d{4}\b", "", title).strip()
            parts = model_text.split()
            if parts:
                v.setdefault("make", parts[0])
                if len(parts) > 1:
                    v.setdefault("model", parts[1])
                if len(parts) > 2:
                    v.setdefault("variant", " ".join(parts[2:]))

    # 4. Price extraction
    price_elm = soup.select_one("[data-testid='vehicle-price'], .vehicle-price, .price")
    if price_elm:
        v["price"] = re.sub(r"[^\d£,]", "", price_elm.get_text(strip=True))

    # 5. Dealer info
    if tel := soup.select_one("a[href^='tel:']"):
        d["phone"] = tel["href"].split("tel:", 1)[-1]
    
    dealer_el = soup.select_one(
        "[data-testid='dealer-name'], "
        "[class*='dealer-name'], "
        "[class*='dealer__name'], "
        ".dealer-info__name"
    )
    if dealer_el:
        d["name"] = dealer_el.get_text(strip=True)
    
    location_el = soup.select_one(
        "[data-testid='dealer-location'], "
        "[class*='dealer-location'], "
        "[class*='dealer__location']"
    )
    if location_el:
        d["location"] = location_el.get_text(strip=True)
        if ", United Kingdom" in d["location"]:
            d["city"] = d["location"].split(",", 1)[0].strip()

    # Print what we found
    print(f"  ✅ Found - Make: {v.get('make')}, "
          f"Model: {v.get('model')}, "
          f"Year: {v.get('year')}, "
          f"Price: {v.get('price')}, "
          f"Dealer: {d.get('name')}")

    return out

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
    batch_size = 3  # Pages per batch
    max_listings_per_page = 10  # Limit listings per page
    delay_between_listings = 2  # seconds
    
    # Updated base URL for AA Cars - using the displaycars endpoint
    base_url = "https://www.theaa.com/used-cars/displaycars"
    search_params = {
        "fullpostcode": "PR267SY",
        "keywordsearch": "",
        "travel": "2000",  # Search radius
        "priceto": "2000",  # Max price
        "page": str(start_page)
    }
    
    end_page = start_page + batch_size - 1
    
    print(f"\n🚀 AA Cars Scraper - Batch Mode")
    print(f"📄 Pages: {start_page} to {end_page}")
    print(f"📊 Total previously scraped: {total_scraped}")
    print("-" * 50)
    
    all_results = []
    current_page = start_page
    
    while current_page <= end_page:
        # Construct search URL
        search_params["page"] = str(current_page)
        search_url = f"{base_url}?{urlencode(search_params)}"
        
        try:
            listing_urls, next_url = await extract_listings_and_next_page(search_url)
            
            if not listing_urls:
                print(f"⚠️ No listings found on page {current_page}")
                # If we have a next URL, update our base URL pattern
                if next_url:
                    parsed_next = urlparse(next_url)
                    base_url = f"{parsed_next.scheme}://{parsed_next.netloc}{parsed_next.path}"
                current_page += 1
                continue
            
            if max_listings_per_page:
                listing_urls = listing_urls[:max_listings_per_page]
            
            page_results = []
            for i, listing_url in enumerate(listing_urls, 1):
                print(f"  [{i}/{len(listing_urls)}]", end="")
                
                try:
                    data = await extract_aa_listing(listing_url)
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
    if len(sys.argv) > 1:
        # Manual mode - scrape specific URLs
        urls = sys.argv[1:]
        asyncio.run(extract_aa_listing(urls[0]))
    else:
        # Batch mode with pagination
        asyncio.run(main())