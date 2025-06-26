"""pistonheads_scraper.py - Fixed with proper data extraction and debugging
• Handles crawl4ai fetch issues
• Better data extraction with debugging
• Proper pagination continuation
"""

import asyncio, re, os, json
from typing import List, Dict, Tuple, Optional
from urllib.parse import urlparse, parse_qs, urlencode
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup
from datetime import datetime

# ──────────────────────────────────────────────────────────────
#  CONSTANTS & JS HELPER
# ──────────────────────────────────────────────────────────────
FETCH_JS = """
    // Wait for page to load
    await new Promise(r => setTimeout(r, 4000));
    
    // Scroll to trigger lazy loading
    window.scrollTo(0, document.body.scrollHeight);
    await new Promise(r => setTimeout(r, 2000));
    
    // Scroll back up
    window.scrollTo(0, 0);
    await new Promise(r => setTimeout(r, 1000));
    
    // Check if content is loaded
    const listings = document.querySelectorAll('a[href*="/buy/listing/"]');
    console.log('Found listings:', listings.length);
"""

# ──────────────────────────────────────────────────────────────
#  DEBUG HELPERS
# ──────────────────────────────────────────────────────────────
def save_debug_html(html: str, filename: str):
    """Save HTML for debugging"""
    debug_dir = "debug_html"
    os.makedirs(debug_dir, exist_ok=True)
    filepath = os.path.join(debug_dir, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"  📄 Debug HTML saved: {filepath}")

# ──────────────────────────────────────────────────────────────
#  PAGE‑LEVEL HELPERS
# ──────────────────────────────────────────────────────────────
async def _fetch_html(url: str, save_debug: bool = False) -> str:
    """Fetch HTML content from URL with retries"""
    for attempt in range(3):
        try:
            async with AsyncWebCrawler(
                verbose=False, 
                headless=True,
                browser_type="chromium",
                page_timeout=60000,
                remove_overlay_elements=True
            ) as crawler:
                res = await crawler.arun(
                    url=url, 
                    timeout=60000, 
                    js_code=FETCH_JS,
                    wait_for_network_idle=True,
                    bypass_cache=True
                )
                
                if res.success and res.html and len(res.html) > 1000:
                    if save_debug:
                        save_debug_html(res.html, f"page_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html")
                    return res.html
                else:
                    print(f"  ⚠️ Attempt {attempt + 1}: Page too small ({len(res.html)} bytes)")
                    
        except Exception as e:
            print(f"  ⚠️ Attempt {attempt + 1} failed: {e}")
            
        if attempt < 2:
            await asyncio.sleep(3)
    
    return ""

async def extract_listings_and_next_page(search_url: str) -> Tuple[List[str], Optional[str]]:
    """Extract listing URLs from search page and determine next page"""
    print(f"🔍 Processing search page: {search_url}")
    
    html = await _fetch_html(search_url, save_debug=True)
    if not html:
        print("❌ Failed to fetch search page")
        return [], None
        
    soup = BeautifulSoup(html, "html.parser")
    urls: List[str] = []
    
    # Debug: Check if we're on the right page
    title = soup.find('title')
    if title:
        print(f"  📄 Page title: {title.text.strip()}")
    
    # Method 1: Direct link search
    all_links = soup.find_all("a", href=True)
    print(f"  🔗 Total links found: {len(all_links)}")
    
    for a in all_links:
        href = a["href"]
        if "/buy/listing/" in href and "thumb" not in href and href != '#':
            full_url = href if href.startswith("http") else f"https://www.pistonheads.com{href}"
            clean_url = full_url.split("?", 1)[0]
            if clean_url not in urls and 'pistonheads.com' in clean_url:
                urls.append(clean_url)
    
    # Method 2: Look for listing containers
    if not urls:
        print("  🔍 Trying alternative selectors...")
        listing_containers = soup.select(
            'div.result-container a, '
            'div.listing a, '
            'article a, '
            'div[class*="result"] a'
        )
        
        for link in listing_containers:
            href = link.get('href', '')
            if href and '/listing/' in href:
                full_url = href if href.startswith("http") else f"https://www.pistonheads.com{href}"
                clean_url = full_url.split("?", 1)[0]
                if clean_url not in urls:
                    urls.append(clean_url)
    
    # Remove duplicates
    urls = list(dict.fromkeys(urls))
    print(f"✅ Found {len(urls)} listings on this page")
    
    if urls:
        print("  📋 First 3 listings:")
        for i, url in enumerate(urls[:3], 1):
            print(f"     {i}. {url}")
    
    # Find next page
    next_url = None
    parsed = urlparse(search_url)
    params = parse_qs(parsed.query)
    current_page = int(params.get("page", ["1"])[0])
    
    # Always construct next page URL if we found listings
    if urls:
        params["page"] = [str(current_page + 1)]
        next_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{urlencode(params, doseq=True)}"
        print(f"➡️ Next page: {next_url}")
    
    return urls, next_url

async def extract_listing_details(listing_url: str) -> Dict:
    """Extract detailed information from a PistonHeads listing"""
    print(f"  🚗 Processing: {listing_url}")
    
    # Fetch with longer timeout for detail pages
    html = await _fetch_html(listing_url)
    if not html:
        print("  ❌ Failed to fetch listing")
        return {}
    
    # Save debug HTML for first few listings
    if "debug_listing" not in globals():
        globals()["debug_listing"] = 0
    if globals()["debug_listing"] < 3:
        save_debug_html(html, f"listing_{globals()['debug_listing']}_{listing_url.split('/')[-1]}.html")
        globals()["debug_listing"] += 1

    soup = BeautifulSoup(html, 'html.parser')
    data = {"listing_url": listing_url, "vehicle": {}, "dealer": {}}
    v, d = data['vehicle'], data['dealer']

    # ===== VEHICLE DETAILS =====
    
    # 1. Title extraction - multiple methods
    title = None
    title_selectors = [
        'h1.advert__title',
        'h1[class*="title"]',
        'h1',
        '.listing-title',
        'meta[property="og:title"]'
    ]
    
    for selector in title_selectors:
        elem = soup.select_one(selector)
        if elem:
            if elem.name == 'meta':
                title = elem.get('content', '').strip()
            else:
                title = elem.get_text(strip=True)
            if title:
                break
    
    if title:
        v['title'] = title
        print(f"    📝 Title: {title}")
        
        # Extract year
        year_match = re.search(r'\b(19[89]\d|20[012]\d)\b', title)
        if year_match:
            v['year'] = year_match.group()
            title_clean = title.replace(year_match.group(), '').strip()
        else:
            title_clean = title
            
        # Extract make, model, variant
        parts = title_clean.split()
        if len(parts) >= 1:
            v['make'] = parts[0]
        if len(parts) >= 2:
            v['model'] = parts[1]
        if len(parts) >= 3:
            v['variant'] = ' '.join(parts[2:])
    else:
        print("    ⚠️ No title found")

    # 2. Price extraction
    price_text = None
    price_selectors = [
        '.advert__price',
        'div[class*="price"]',
        'span[class*="price"]',
        '.listing-price',
        'meta[property="product:price:amount"]'
    ]
    
    for selector in price_selectors:
        elem = soup.select_one(selector)
        if elem:
            if elem.name == 'meta':
                price_text = elem.get('content', '')
            else:
                price_text = elem.get_text(strip=True)
            if price_text and ('£' in price_text or price_text.isdigit()):
                break
    
    if price_text:
        if price_match := re.search(r'£?([\d,]+)', price_text):
            v['price'] = f"£{price_match.group(1)}"
            print(f"    💰 Price: {v['price']}")

    # 3. Specifications - Look for structured data
    specs_found = False
    
    # Method 1: Definition lists (common on PH)
    for dl in soup.find_all('dl'):
        dts = dl.find_all('dt')
        dds = dl.find_all('dd')
        if len(dts) == len(dds):
            for dt, dd in zip(dts, dds):
                label = dt.get_text(strip=True).lower()
                value = dd.get_text(strip=True)
                
                if 'year' in label:
                    v['year'] = value
                    specs_found = True
                elif 'mileage' in label:
                    v['mileage'] = re.sub(r'[^\d]', '', value)
                    specs_found = True
                elif 'fuel' in label:
                    v['fuel_type'] = value
                    specs_found = True
                elif 'transmission' in label or 'gearbox' in label:
                    v['gearbox'] = value
                    specs_found = True
                elif 'body' in label:
                    v['body_type'] = value
                    specs_found = True
    
    # Method 2: Key-value pairs in lists
    if not specs_found:
        for container in soup.select('ul.key-facts li, ul.specs li, div.specs-list li'):
            text = container.get_text(' ', strip=True).lower()
            
            # Mileage
            if mileage_match := re.search(r'([\d,]+)\s*miles', text):
                v['mileage'] = mileage_match.group(1).replace(',', '')
            
            # Year
            if year_match := re.search(r'(19[89]\d|20[012]\d)', text):
                v.setdefault('year', year_match.group(1))
            
            # Fuel
            for fuel in ['petrol', 'diesel', 'electric', 'hybrid']:
                if fuel in text:
                    v.setdefault('fuel_type', fuel.title())
                    break
            
            # Gearbox
            for gb in ['manual', 'automatic', 'semi-auto']:
                if gb in text:
                    v.setdefault('gearbox', gb.capitalize())
                    break

    # Method 3: Generic text search as fallback
    if not v.get('mileage') or not v.get('fuel_type'):
        all_text = soup.get_text(' ', strip=True).lower()
        
        if not v.get('mileage'):
            if mileage_match := re.search(r'([\d,]+)\s*miles', all_text):
                v['mileage'] = mileage_match.group(1).replace(',', '')
        
        if not v.get('fuel_type'):
            for fuel in ['petrol', 'diesel', 'electric', 'hybrid']:
                if fuel in all_text:
                    v['fuel_type'] = fuel.title()
                    break

    # ===== DEALER DETAILS =====
    
    # Dealer name
    dealer_selectors = [
        '.seller-name',
        'h3.dealer-name',
        'div[class*="seller"] h3',
        '.dealer-info h3',
        'div[class*="dealer"] h2'
    ]
    
    for selector in dealer_selectors:
        if dealer_elem := soup.select_one(selector):
            d['name'] = dealer_elem.get_text(strip=True)
            print(f"    🏢 Dealer: {d['name']}")
            break
    
    # Phone number
    if tel := soup.find('a', href=re.compile(r'tel:')):
        d['phone'] = tel['href'].split(':', 1)[1]
        print(f"    📞 Phone: {d['phone']}")
    
    # Location
    location_selectors = [
        '.seller-location',
        '.dealer-location',
        'div[class*="location"]'
    ]
    
    for selector in location_selectors:
        if loc_elem := soup.select_one(selector):
            location = loc_elem.get_text(strip=True)
            d['location'] = location
            if ',' in location:
                d['city'] = location.split(',')[0].strip()
            break

    # Summary of what we found
    print(f"  ✅ Extracted: {v.get('make', 'Unknown')} {v.get('model', '')} {v.get('year', '')} - {v.get('price', 'N/A')}")
    
    # Only return if we have at least some vehicle data
    return data if (v.get('title') or v.get('price') or v.get('make')) else {}

# ──────────────────────────────────────────────────────────────
#  PUBLIC BATCH RUNNER (for orchestrator)
# ──────────────────────────────────────────────────────────────
async def run_pistonheads(batch_pages: int = 5, start_page: int = 1, max_per_page: int = 60) -> List[Dict]:
    """
    Main entry point for PistonHeads scraper
    - Scrapes multiple pages of search results
    - Extracts detailed information from each listing
    """
    rows: List[Dict] = []
    current_page = start_page
    pages_processed = 0
    consecutive_empty_pages = 0
    
    # Initial search URL
    search_url = (
        f"https://www.pistonheads.com/buy/search"
        f"?price=0&price=2000&seller-type=Trade"
        f"&page={current_page}"
    )
    
    print(f"🚗 Starting PistonHeads scraper")
    print(f"📄 Pages: {start_page} to {start_page + batch_pages - 1}")
    print(f"🔗 Base URL: {search_url}")
    print("-" * 60)
    
    while pages_processed < batch_pages and consecutive_empty_pages < 2:
        print(f"\n📄 Page {current_page}:")
        
        # Get listings from current page
        listing_urls, next_url = await extract_listings_and_next_page(search_url)
        
        if not listing_urls:
            consecutive_empty_pages += 1
            print(f"⚠️ No listings found (empty pages: {consecutive_empty_pages})")
            
            if consecutive_empty_pages >= 2:
                print("❌ Two consecutive empty pages, stopping")
                break
        else:
            consecutive_empty_pages = 0
        
        # Limit listings per page if specified
        urls_to_process = listing_urls[:max_per_page] if max_per_page else listing_urls
        
        # Extract details from each listing
        page_results = []
        for i, url in enumerate(urls_to_process, 1):
            try:
                print(f"  [{i}/{len(urls_to_process)}]", end="")
                rec = await extract_listing_details(url)
                if rec and rec.get('vehicle'):
                    rows.append(rec)
                    page_results.append(rec)
            except Exception as exc:
                print(f" ❌ Error: {exc}")
            
            # Rate limiting
            if i < len(urls_to_process):
                await asyncio.sleep(2)
        
        print(f"\n✅ Page {current_page} complete: {len(page_results)} valid listings")
        
        pages_processed += 1
        current_page += 1
        
        # Update search URL for next iteration
        if pages_processed < batch_pages:
            search_url = (
                f"https://www.pistonheads.com/buy/search"
                f"?price=0&price=2000&seller-type=Trade"
                f"&page={current_page}"
            )
            
            # Rate limiting between pages
            await asyncio.sleep(3)
    
    print(f"\n{'='*60}")
    print(f"✅ PistonHeads scraping complete!")
    print(f"📊 Total listings collected: {len(rows)}")
    print(f"📄 Pages processed: {pages_processed}")
    print(f"📄 Last page: {current_page - 1}")
    print(f"{'='*60}")
    
    return rows

# ──────────────────────────────────────────────────────────────
#  CLI TEST
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1].startswith("http"):
        # Test specific URLs
        targets = sys.argv[1:]
        async def _single(urls):
            results = []
            for url in urls:
                result = await extract_listing_details(url)
                results.append(result)
            return results
        
        results = asyncio.run(_single(targets))
        print(json.dumps(results, indent=2))
    else:
        # Test batch scraping
        if len(sys.argv) > 1:
            start_page = int(sys.argv[1])
            batch_pages = int(sys.argv[2]) if len(sys.argv) > 2 else 2
        else:
            start_page = 1
            batch_pages = 2
            
        demo_rows = asyncio.run(run_pistonheads(batch_pages=batch_pages, start_page=start_page))
        if demo_rows:
            print("\n📋 Sample data:")
            print(json.dumps(demo_rows[:2], indent=2))
        else:
            print("❌ No data collected")