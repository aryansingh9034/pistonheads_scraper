"""cazoo_scraper.py - Scraper for Cazoo car listings
• Handles Cazoo's specific HTML structure
• Implements pagination correctly
• Extracts detailed vehicle and dealer information
"""

import asyncio
import re
import json
import sys
import os
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse, parse_qs, urlencode
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup
from datetime import datetime
from db_helper import save_to_leads

# JavaScript to ensure page loads properly
FETCH_JS = """
    // Wait for page to load
    await new Promise(r => setTimeout(r, 5000));
    
    // Scroll to trigger lazy loading
    window.scrollTo(0, document.body.scrollHeight);
    await new Promise(r => setTimeout(r, 3000));
    
    // Scroll back up
    window.scrollTo(0, 0);
    await new Promise(r => setTimeout(r, 2000));
    
    // Try to trigger any lazy loading
    const scrollHeight = document.body.scrollHeight;
    for (let i = 0; i < 3; i++) {
        window.scrollTo(0, scrollHeight * (i + 1) / 3);
        await new Promise(r => setTimeout(r, 1000));
    }
    
    // Check if content is loaded
    const listings = document.querySelectorAll('a[href*="/cars-for-sale/"]');
    console.log('Found listings:', listings.length);
    
    // Return the HTML content
    return document.documentElement.outerHTML;
"""

# Default search URL with max price filter
DEFAULT_SEARCH_URL = "https://www.cazoo.co.uk/cars/?page=1"
MAX_PAGES_HARVEST = 40  # Maximum pages to scrape when no URLs provided

# ──────────────────────────────────────────────────────────────
#  HARVESTER – collect car listing links from Cazoo search pages
# ──────────────────────────────────────────────────────────────
async def _harvest_links(search_url: str, max_pages: int) -> List[str]:
    """Harvest all car listing URLs from Cazoo search pages"""
    all_links = []
    page = 1
    consecutive_empty = 0
    
    while page <= max_pages and consecutive_empty < 2:
        print(f"🔍 Harvesting page {page}...")
        
        # Update page number in URL
        parsed = urlparse(search_url)
        params = parse_qs(parsed.query)
        params['page'] = [str(page)]
        current_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{urlencode(params, doseq=True)}"
        
        print(f"  📄 Fetching: {current_url}")
        
        async with AsyncWebCrawler(
            verbose=False, 
            headless=True,
            browser_type="chromium",
            page_timeout=90000,
            remove_overlay_elements=True
        ) as crawler:
            res = await crawler.arun(
                url=current_url, 
                timeout=90000,
                js_code=FETCH_JS,
                wait_for_network_idle=True,
                bypass_cache=True
            )
        
        if not res.success:
            print(f"❌ Failed to fetch page {page}")
            consecutive_empty += 1
            if consecutive_empty >= 2:
                break
            page += 1
            continue
            
        print(f"  📄 Page size: {len(res.html)} bytes")
        
        soup = BeautifulSoup(res.html, "html.parser")
        page_links = []
        
        # Debug: Check page title and structure
        title = soup.find('title')
        if title:
            print(f"  📝 Page title: {title.text.strip()}")
        
        # Look for all links with /cars-for-sale/ pattern
        print("  🔍 Searching for car listing links...")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            
            # Cazoo uses /cars-for-sale/[ID] pattern
            if '/cars-for-sale/' in href and href.count('/') >= 3:
                # Check if it has an ID (numeric part)
                if re.search(r'/cars-for-sale/\d+', href):
                    full_url = href if href.startswith("http") else f"https://www.cazoo.co.uk{href}"
                    # Remove query parameters
                    clean_url = full_url.split("?", 1)[0].split("#", 1)[0]
                    
                    if clean_url not in page_links and 'cazoo.co.uk' in clean_url:
                        page_links.append(clean_url)
        
        print(f"    Found {len(page_links)} car listing links")
        
        # Remove duplicates while preserving order
        page_links = list(dict.fromkeys(page_links))
        
        if not page_links:
            print(f"⚠️ No listings found on page {page}")
            consecutive_empty += 1
            
            # Debug: Save HTML for inspection
            debug_dir = "debug_html"
            os.makedirs(debug_dir, exist_ok=True)
            debug_file = os.path.join(debug_dir, f"cazoo_page_{page}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html")
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(res.html)
            print(f"  📄 Debug HTML saved: {debug_file}")
        else:
            consecutive_empty = 0
            all_links.extend(page_links)
            print(f"✅ Found {len(page_links)} listings on page {page}")
            
            # Show first few links
            for i, link in enumerate(page_links[:3], 1):
                print(f"    {i}. {link}")
        
        page += 1
        await asyncio.sleep(2)  # Rate limiting
    
    # Remove duplicates while preserving order
    unique_links = list(dict.fromkeys(all_links))
    return unique_links

# ──────────────────────────────────────────────────────────────
#  SINGLE LISTING PARSER - Extract details from Cazoo listing
# ──────────────────────────────────────────────────────────────
async def extract_cazoo_listing(url: str) -> Dict:
    """Extract detailed information from a single Cazoo listing"""
    async with AsyncWebCrawler(
        verbose=False, 
        headless=True,
        browser_type="chromium",
        page_timeout=60000
    ) as crawler:
        res = await crawler.arun(
            url=url, 
            timeout=60000, 
            js_code=FETCH_JS,
            wait_for_network_idle=True
        )
    
    if not res.success:
        print(f"❌ Fetch failed {url}")
        return {}
    
    soup = BeautifulSoup(res.html, "html.parser")
    
    # Check if it's an error page
    page_text = soup.get_text()
    if "taken a wrong turn" in page_text or "page not found" in page_text.lower() or "404" in page_text:
        print(f"❌ Error page detected for {url}")
        return {}
    
    row: Dict = {"listing_url": url, "vehicle": {}, "dealer": {}}
    v, d = row["vehicle"], row["dealer"]
    
    # 1. Title extraction - be more specific
    title = None
    title_found = False
    
    # First try structured data
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string)
            if isinstance(data, dict):
                if data.get('@type') in ['Car', 'Product', 'Vehicle']:
                    if 'name' in data:
                        title = data['name']
                        title_found = True
                        break
                    # Also check offers
                    if 'offers' in data and isinstance(data['offers'], dict):
                        if 'name' in data['offers']:
                            title = data['offers']['name']
                            title_found = True
                            break
        except:
            pass
    
    # If not found in structured data, try specific selectors
    if not title_found:
        title_selectors = [
            'h1[data-test*="title"]',
            'h1[data-test*="vehicle"]',
            'h1[data-test*="car"]',
            'h1.vehicle-title',
            'h1.car-title',
            'h1[class*="vehicle"]',
            'h1[class*="title"]:not([class*="dealer"])',
            'div[class*="vehicle-header"] h1',
            'section[class*="vehicle"] h1',
            'meta[property="og:title"]'
        ]
        
        for selector in title_selectors:
            elem = soup.select_one(selector)
            if elem:
                if elem.name == 'meta':
                    title = elem.get('content', '').strip()
                else:
                    title = elem.get_text(strip=True)
                
                # Validate it's a real car title
                if title and len(title) > 5 and not any(skip in title.lower() for skip in ['cookie', 'privacy', 'error', '404', 'not found']):
                    title_found = True
                    break
    
    # Clean and parse title
    if title:
        # Remove common prefixes
        title = re.sub(r'^(Used|Nearly New)\s+', '', title, flags=re.I)
        title = title.strip()
        v["title"] = title
        
        # Extract year - handle both full years and UK registration codes
        year_match = re.search(r'\b(19[89]\d|20[0-2]\d)\b', title)
        reg_year_match = re.search(r'\((\d{2})\)', title)  # UK reg year like (67)
        
        if year_match:
            v["year"] = year_match.group(1)
            # Remove year from title for parsing
            title_parts = title.replace(year_match.group(), '').strip()
        elif reg_year_match:
            # Convert UK registration year to full year
            reg_year = int(reg_year_match.group(1))
            if reg_year >= 51:
                v["year"] = str(2000 + reg_year)
            else:
                v["year"] = str(2000 + reg_year)
            # Remove reg year from title
            title_parts = re.sub(r'\(\d{2}\)', '', title).strip()
        else:
            title_parts = title
        
        # Try to parse make/model/variant more intelligently
        # First, try to find known car makes
        known_makes = [
            'Audi', 'BMW', 'Mercedes-Benz', 'Mercedes', 'Volkswagen', 'VW', 'Ford', 
            'Vauxhall', 'Nissan', 'Toyota', 'Honda', 'Mazda', 'Hyundai', 'Kia',
            'Peugeot', 'Citroen', 'Renault', 'Fiat', 'Volvo', 'Skoda', 'Seat',
            'Land Rover', 'Jaguar', 'Mini', 'Porsche', 'Lexus', 'Mitsubishi',
            'Suzuki', 'Subaru', 'Chevrolet', 'Jeep', 'Alfa Romeo', 'Dacia',
            'Smart', 'MG', 'Infiniti', 'Bentley', 'Aston Martin', 'Ferrari',
            'Lamborghini', 'Maserati', 'McLaren', 'Rolls-Royce', 'Tesla'
        ]
        
        make_found = False
        for make in known_makes:
            if make.lower() in title_parts.lower():
                v["make"] = make
                make_found = True
                # Remove make from title to get model
                title_parts = re.sub(re.escape(make), '', title_parts, flags=re.I).strip()
                break
        
        if not make_found:
            # If no known make found, use first word as make
            words = title_parts.split()
            if words:
                v["make"] = words[0]
                title_parts = ' '.join(words[1:]) if len(words) > 1 else ''
        
        # Extract model and variant from remaining parts
        remaining_words = title_parts.split()
        if remaining_words:
            # First remaining word is likely the model
            v["model"] = remaining_words[0]
            
            # Rest is variant (filter out common non-variant words)
            if len(remaining_words) > 1:
                variant_parts = remaining_words[1:]
                variant_parts = [p for p in variant_parts if p.lower() not in 
                                ['for', 'sale', 'used', 'new', '-', '–', '—']]
                if variant_parts:
                    v["variant"] = ' '.join(variant_parts)
    
    # 2. Price extraction - more thorough
    price_found = False
    
    # First check structured data
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string)
            if isinstance(data, dict) and 'offers' in data:
                offers = data['offers']
                if isinstance(offers, dict) and 'price' in offers:
                    price = str(offers['price'])
                    if price.replace('.', '').isdigit():
                        v["price"] = f"£{int(float(price)):,}"
                        price_found = True
                        break
        except:
            pass
    
    # Then try selectors
    if not price_found:
        price_selectors = [
            '[data-test*="price"]:not([data-test*="finance"])',
            '[data-test*="vehicle-price"]',
            'span[class*="price"]:not([class*="finance"])',
            'div[class*="price"]:not([class*="finance"])',
            '.vehicle-price',
            '.car-price',
            'h2[class*="price"]',
            'p[class*="price"]'
        ]
        
        for selector in price_selectors:
            elem = soup.select_one(selector)
            if elem:
                price_text = elem.get_text(strip=True)
                # Extract price - look for patterns
                price_match = re.search(r'£\s*([\d,]+)', price_text)
                if price_match:
                    price_value = price_match.group(1).replace(',', '')
                    if price_value.isdigit() and 100 <= int(price_value) <= 1000000:
                        v["price"] = f"£{price_match.group(1)}"
                        price_found = True
                        break
    
    # 3. Vehicle specifications - more targeted extraction
    
    # Mileage
    mileage_patterns = [
        r'(\d{1,3}(?:,\d{3})*)\s*miles',
        r'mileage[:\s]+(\d{1,3}(?:,\d{3})*)',
        r'(\d{1,3}(?:,\d{3})*)\s*mi\b'
    ]
    
    for pattern in mileage_patterns:
        if match := re.search(pattern, page_text, re.I):
            mileage = match.group(1).replace(',', '')
            if mileage.isdigit() and int(mileage) < 500000:  # Reasonable mileage
                v["mileage"] = mileage
                break
    
    # Fuel type - look in spec sections
    spec_sections = soup.select('[class*="spec"], [class*="detail"], [data-test*="spec"]')
    for section in spec_sections:
        section_text = section.get_text(strip=True).lower()
        for fuel in ['petrol', 'diesel', 'electric', 'hybrid', 'plug-in hybrid', 'phev']:
            if fuel in section_text:
                v["fuel_type"] = fuel.title().replace('Phev', 'PHEV')
                break
    
    # Transmission
    for section in spec_sections:
        section_text = section.get_text(strip=True).lower()
        for trans in ['manual', 'automatic', 'semi-automatic', 'cvt', 'dsg']:
            if trans in section_text:
                v["gearbox"] = trans.capitalize()
                break
    
    # Body type
    for section in spec_sections:
        section_text = section.get_text(strip=True).lower()
        for body in ['hatchback', 'saloon', 'estate', 'suv', 'mpv', 'coupe', 'convertible', 'van', 'pickup']:
            if body in section_text:
                v["body_type"] = body.capitalize()
                break
    
    # 4. Dealer information - be very specific
    dealer_found = False
    
    # Look for dealer in structured data first
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string)
            if isinstance(data, dict):
                # Check for seller in offers
                if 'offers' in data and isinstance(data['offers'], dict):
                    if 'seller' in data['offers'] and isinstance(data['offers']['seller'], dict):
                        seller = data['offers']['seller']
                        if 'name' in seller:
                            d["name"] = seller['name']
                            dealer_found = True
                        if 'address' in seller:
                            if isinstance(seller['address'], dict):
                                address_parts = []
                                for key in ['streetAddress', 'addressLocality', 'addressRegion', 'postalCode']:
                                    if key in seller['address']:
                                        address_parts.append(seller['address'][key])
                                if address_parts:
                                    d["location"] = ', '.join(address_parts)
                                    if 'addressLocality' in seller['address']:
                                        d["city"] = seller['address']['addressLocality']
                        if 'telephone' in seller:
                            d["phone"] = seller['telephone']
        except:
            pass
    
    # If not found in structured data, look for dealer sections
    if not dealer_found:
        dealer_sections = soup.select(
            '[data-test*="dealer"], '
            '[data-test*="seller"], '
            'section[class*="dealer"], '
            'div[class*="dealer-info"], '
            'div[class*="seller-info"], '
            'div[class*="dealership"]'
        )
        
        for section in dealer_sections:
            # Look for dealer name
            name_elem = section.select_one('h2, h3, h4, strong, [class*="name"]')
            if name_elem:
                dealer_name = name_elem.get_text(strip=True)
                # Validate it's a real dealer name
                if dealer_name and len(dealer_name) > 2 and not any(skip in dealer_name.lower() for skip in ['view', 'call', 'message', 'contact']):
                    d["name"] = dealer_name
                    dealer_found = True
                    
                    # Look for location in the same section
                    location_elem = section.select_one('[class*="location"], [class*="address"], address')
                    if location_elem:
                        location = location_elem.get_text(strip=True)
                        if location and len(location) > 5:
                            d["location"] = location
                            if ',' in location:
                                d["city"] = location.split(',')[0].strip()
                    
                    # Look for phone
                    phone_elem = section.select_one('a[href^="tel:"]')
                    if phone_elem:
                        d["phone"] = phone_elem['href'].replace('tel:', '').strip()
                    
                    break
    
    # Last resort - look for specific text patterns
    if not dealer_found:
        # Look for "Sold by" pattern
        sold_by_match = re.search(r'Sold by[:\s]+([A-Za-z0-9\s&\-\.]+?)(?:\n|<|$)', page_text)
        if sold_by_match:
            dealer_name = sold_by_match.group(1).strip()
            if dealer_name and len(dealer_name) > 2:
                d["name"] = dealer_name
                dealer_found = True
    
    # If still no dealer found, default to Cazoo
    if not d.get("name"):
        d["name"] = "Cazoo Marketplace"
        d["note"] = "Dealer information not available"
    
    # Clean up phone number if found
    if d.get("phone"):
        # Remove any non-digit characters except + and spaces
        phone = re.sub(r'[^\d\s+]', '', d["phone"])
        d["phone"] = phone.strip()
    
    # Ensure we have the listing URL
    row["listing_url"] = url
    
    # Only return if we have meaningful vehicle data
    if v.get("title") or v.get("make") or v.get("price"):
        print(f"✅ Extracted: {v.get('make', 'Unknown')} {v.get('model', '')} {v.get('year', '')} - {v.get('price', 'N/A')} from {d.get('name', 'Unknown dealer')}")
        return row
    else:
        print(f"⚠️ No valid vehicle data found for {url}")
        return {}

# ──────────────────────────────────────────────────────────────
#  PUBLIC BATCH RUNNER with pagination support
# ──────────────────────────────────────────────────────────────
async def run_cazoo(
    urls: Optional[List[str]] = None, 
    batch_size: int = 100,
    batch_pages: int = 3,
    start_page: int = 1,
    max_per_page: int = 50
) -> List[Dict]:
    """
    Main entry point for Cazoo scraper
    - If urls provided: scrape those specific listings
    - Otherwise: harvest from search pages and scrape
    - Supports pagination with start_page parameter
    """
    if urls is None:
        print("🔍 Starting Cazoo harvest...")
        print(f"📄 Pages: {start_page} to {start_page + batch_pages - 1}")
        
        # Harvest only the specified pages
        urls = []
        for page_num in range(start_page, start_page + batch_pages):
            page_url = f"https://www.cazoo.co.uk/cars/?page={page_num}"
            print(f"\n📄 Harvesting page {page_num}...")
            
            page_urls = await _harvest_links(page_url, 1)  # Harvest just this page
            urls.extend(page_urls)
            
            if not page_urls:
                print(f"⚠️ No URLs found on page {page_num}")
            
            await asyncio.sleep(1.5)  # Rate limiting between pages
        
        print(f"\n✅ Harvested {len(urls)} total URLs from {batch_pages} pages")
        
        if not urls:
            print("❌ No URLs harvested!")
            return []
    
    # Limit URLs to process
    urls_to_scrape = urls[:batch_size] if batch_size else urls
    
    # Filter to only actual car listing URLs
    car_listing_urls = []
    for url in urls_to_scrape:
        if '/cars-for-sale/' in url and re.search(r'/\d+', url):  # Has numeric ID
            car_listing_urls.append(url)
        else:
            print(f"  ⏭️ Skipping non-car listing: {url}")
    
    print(f"\n🚗 Scraping {len(car_listing_urls)} actual car listings...")
    
    rows: List[Dict] = []
    batch_save_size = 10  # Save every 10 records
    
    for i, url in enumerate(car_listing_urls, 1):
        try:
            print(f"[{i}/{len(car_listing_urls)}] ", end="")
            rec = await extract_cazoo_listing(url)
            if rec and rec.get("vehicle"):
                rows.append(rec)
                
                # Save in batches
                if len(rows) >= batch_save_size:
                    save_to_leads(rows, 'cazoo')
                    rows = []  # Clear the batch
            else:
                print(f"⚠️ No valid data from {url}")
        except Exception as e:
            print(f"❌ Error on {url}: {e}")
        
        # Rate limiting
        if i < len(car_listing_urls):
            await asyncio.sleep(2)
    
    # Save any remaining records
    if rows:
        save_to_leads(rows, 'cazoo')
    
    print(f"\n✅ Successfully scraped {len(car_listing_urls)} listings")
    
    # Return all scraped records for the caller
    return rows

# ──────────────────────────────────────────────────────────────
#  CLI TEST
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].startswith("http"):
        # Test specific URLs
        test_urls = sys.argv[1:]
        results = asyncio.run(run_cazoo(test_urls, batch_size=len(test_urls)))
        print(json.dumps(results, indent=2))
    else:
        # Test harvest and scrape
        if len(sys.argv) > 1:
            start_page = int(sys.argv[1])
            batch_pages = int(sys.argv[2]) if len(sys.argv) > 2 else 2
        else:
            start_page = 1
            batch_pages = 2
            
        data = asyncio.run(run_cazoo(
            batch_size=50,
            batch_pages=batch_pages,
            start_page=start_page
        ))
        
        if data:
            print("\n📋 Sample data:")
            print(json.dumps(data[:2], indent=2))
            print(f"\n✅ Total: {len(data)} listings")
        else:
            print("❌ No data collected")