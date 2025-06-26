"""pistonheads_scraper.py - Updated with better pagination and extraction
• Properly handles PistonHeads pagination
• Extracts more vehicle and dealer details
• More robust error handling
"""

import asyncio, re, os, json
from typing import List, Dict, Tuple, Optional
from urllib.parse import urlparse, parse_qs, urlencode
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup

# ──────────────────────────────────────────────────────────────
#  CONSTANTS & JS HELPER
# ──────────────────────────────────────────────────────────────
FETCH_JS = """
    await new Promise(r => setTimeout(r, 3000));
    window.scrollTo(0, document.body.scrollHeight);
    await new Promise(r => setTimeout(r, 2000));
    window.scrollTo(0, 0);
    await new Promise(r => setTimeout(r, 1000));
"""

# ──────────────────────────────────────────────────────────────
#  PAGE‑LEVEL HELPERS
# ──────────────────────────────────────────────────────────────
async def _fetch_html(url: str) -> str:
    """Fetch HTML content from URL"""
    async with AsyncWebCrawler(verbose=False, headless=True) as crawler:
        res = await crawler.arun(url=url, timeout=45000, js_code=FETCH_JS)
    return res.html if res.success else ""

async def extract_listings_and_next_page(search_url: str) -> Tuple[List[str], Optional[str]]:
    """Extract listing URLs from search page and determine next page"""
    print(f"🔍 Processing search page: {search_url}")
    
    html = await _fetch_html(search_url)
    if not html:
        print("❌ Failed to fetch search page")
        return [], None
        
    soup = BeautifulSoup(html, "html.parser")
    urls: List[str] = []
    
    # Method 1: Look for listing links
    listing_selectors = [
        'a[href*="/buy/listing/"]',
        'div.listing-card a',
        'article.listing a',
        'div[class*="result-item"] a',
        'div[class*="listing-item"] a'
    ]
    
    for selector in listing_selectors:
        links = soup.select(selector)
        for link in links:
            href = link.get('href', '')
            if href and '/buy/listing/' in href and 'thumb' not in href:
                full_url = href if href.startswith("http") else f"https://www.pistonheads.com{href}"
                clean_url = full_url.split("?", 1)[0]
                if clean_url not in urls:
                    urls.append(clean_url)
    
    # Method 2: Find all links with listing pattern
    if not urls:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/buy/listing/" in href and "thumb" not in href and href != '#':
                full_url = href if href.startswith("http") else f"https://www.pistonheads.com{href}"
                clean_url = full_url.split("?", 1)[0]
                if clean_url not in urls:
                    urls.append(clean_url)
    
    # Remove duplicates
    urls = list(dict.fromkeys(urls))
    print(f"✅ Found {len(urls)} listings on this page")
    
    # Find next page
    next_url = None
    
    # Method 1: Look for pagination links
    pagination_selectors = [
        'a[aria-label*="Next"]',
        'a.next',
        'a[class*="pagination-next"]',
        'nav.pagination a',
        '.paging a'
    ]
    
    for selector in pagination_selectors:
        next_links = soup.select(selector)
        for link in next_links:
            href = link.get('href', '')
            link_text = link.get_text(strip=True).lower()
            if href and ('next' in link_text or '>' in link_text or '»' in link_text):
                next_url = href if href.startswith("http") else f"https://www.pistonheads.com{href}"
                break
        if next_url:
            break
    
    # Method 2: Calculate next page from current URL
    if not next_url and urls:  # Only if we found listings
        parsed = urlparse(search_url)
        params = parse_qs(parsed.query)
        current_page = int(params.get("page", ["1"])[0])
        params["page"] = [str(current_page + 1)]
        next_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{urlencode(params, doseq=True)}"
    
    if next_url:
        print(f"➡️ Next page: {next_url}")
    
    return urls, next_url

async def extract_listing_details(listing_url: str) -> Dict:
    """Extract detailed information from a PistonHeads listing"""
    print(f"  🚗 Processing: {listing_url}")
    
    async with AsyncWebCrawler(verbose=False, headless=True) as crawler:
        result = await crawler.arun(url=listing_url, timeout=45000, js_code=FETCH_JS)
    
    if not result.success:
        print("  ❌ Failed to fetch listing")
        return {}

    soup = BeautifulSoup(result.html, 'html.parser')
    data = {"listing_url": listing_url, "vehicle": {}, "dealer": {}}
    v, d = data['vehicle'], data['dealer']

    # ===== VEHICLE DETAILS =====
    
    # 1. Title extraction
    title_selectors = ['h1', 'h1.advert-title', '.listing-title h1']
    for selector in title_selectors:
        if title_elem := soup.select_one(selector):
            title = title_elem.get_text(strip=True)
            v['title'] = title
            
            # Extract year
            if year_match := re.search(r'\b(19|20)\d{2}\b', title):
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
            break

    # 2. Price extraction
    price_selectors = [
        '.price-text',
        'span[class*="price"]',
        'div[class*="price"]',
        '.advert-price'
    ]
    
    for selector in price_selectors:
        if price_elem := soup.select_one(selector):
            price_text = price_elem.get_text(strip=True)
            if price_match := re.search(r'£([\d,]+)', price_text):
                v['price'] = f"£{price_match.group(1)}"
                break
    
    # Fallback price search
    if 'price' not in v:
        if price_elem := soup.find(string=re.compile(r'£[\d,]+')):
            if price_match := re.search(r'£([\d,]+)', str(price_elem)):
                v['price'] = f"£{price_match.group(1)}"

    # 3. Specifications extraction
    
    # Look for spec containers
    spec_containers = soup.select(
        'ul.specs li, '
        'div.key-facts li, '
        'div[class*="specification"] li, '
        'dl.details dt, '
        'dl.details dd'
    )
    
    # Build spec text for pattern matching
    spec_text = ' '.join(elem.get_text(' ', strip=True).lower() for elem in spec_containers)
    
    # Also include any list items that might contain specs
    all_li = soup.find_all('li')
    spec_text += ' ' + ' '.join(li.get_text(strip=True).lower() for li in all_li)
    
    # Extract mileage
    mileage_patterns = [
        r'([\d,]+)\s*miles',
        r'mileage[:\s]*([\d,]+)',
        r'([\d,]+)\s*mi\b'
    ]
    
    for pattern in mileage_patterns:
        if match := re.search(pattern, spec_text):
            v['mileage'] = match.group(1).replace(',', '')
            break
    
    # Extract fuel type
    fuel_types = ['petrol', 'diesel', 'electric', 'hybrid', 'plug-in hybrid', 'lpg', 'phev']
    for fuel in fuel_types:
        if fuel in spec_text:
            v['fuel_type'] = fuel.title()
            break
    
    # Extract gearbox
    gearbox_types = ['manual', 'automatic', 'semi-automatic', 'cvt', 'dsg', 'tiptronic']
    for gb in gearbox_types:
        if gb in spec_text:
            v['gearbox'] = gb.capitalize()
            break
    
    # Extract body type
    body_types = [
        'hatchback', 'saloon', 'estate', 'suv', 'coupe', 'convertible',
        'mpv', 'pickup', 'van', '4x4', 'sports', 'cabriolet', 'roadster'
    ]
    for body in body_types:
        if body in spec_text:
            v['body_type'] = body.title()
            break

    # 4. Try to find structured specs
    # PistonHeads sometimes uses definition lists
    dt_elements = soup.find_all('dt')
    dd_elements = soup.find_all('dd')
    
    if len(dt_elements) == len(dd_elements):
        for dt, dd in zip(dt_elements, dd_elements):
            label = dt.get_text(strip=True).lower()
            value = dd.get_text(strip=True)
            
            if 'year' in label and 'year' not in v:
                v['year'] = value
            elif 'mileage' in label and 'mileage' not in v:
                v['mileage'] = re.sub(r'[^\d]', '', value)
            elif 'fuel' in label and 'fuel_type' not in v:
                v['fuel_type'] = value
            elif ('transmission' in label or 'gearbox' in label) and 'gearbox' not in v:
                v['gearbox'] = value
            elif 'body' in label and 'body_type' not in v:
                v['body_type'] = value

    # ===== DEALER DETAILS =====
    
    # Dealer name
    dealer_selectors = [
        'div.SellerDetails_tradeSellerWrapper h3',
        '.seller-name',
        'h3[class*="dealer"]',
        '.dealer-info h3',
        'div[class*="seller"] h3'
    ]
    
    for selector in dealer_selectors:
        if dealer_elem := soup.select_one(selector):
            d['name'] = dealer_elem.get_text(strip=True)
            break
    
    # Phone number
    if tel := soup.find('a', href=re.compile(r'tel:')):
        d['phone'] = tel['href'].split(':', 1)[1]
    
    # Location
    location_selectors = [
        '.seller-location',
        'div[class*="location"]',
        '.dealer-address'
    ]
    
    for selector in location_selectors:
        if loc_elem := soup.select_one(selector):
            location = loc_elem.get_text(strip=True)
            d['location'] = location
            if ',' in location:
                d['city'] = location.split(',')[0].strip()
            break
    
    # Alternative location search
    if 'location' not in d:
        if loc := soup.find(string=re.compile(r',\s*United Kingdom')):
            loc_txt = str(loc).strip()
            d['location'] = loc_txt
            d['city'] = loc_txt.split(',', 1)[0]

    print(f"  ✅ Extracted: {v.get('make')} {v.get('model')} {v.get('year')} - {v.get('price', 'N/A')}")
    
    return data if v else {}

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
    
    # Initial search URL
    search_url = (
        f"https://www.pistonheads.com/buy/search"
        f"?price=0&price=2000&seller-type=Trade"
        f"&page={current_page}"
    )
    
    print(f"🚗 Starting PistonHeads scraper - {batch_pages} pages from page {start_page}")
    
    while pages_processed < batch_pages and search_url:
        print(f"\n📄 Page {current_page}:")
        
        # Get listings from current page
        listing_urls, next_url = await extract_listings_and_next_page(search_url)
        
        if not listing_urls:
            print("⚠️ No listings found, stopping")
            break
        
        # Limit listings per page if specified
        urls_to_process = listing_urls[:max_per_page] if max_per_page else listing_urls
        
        # Extract details from each listing
        for i, url in enumerate(urls_to_process, 1):
            try:
                print(f"  [{i}/{len(urls_to_process)}]", end="")
                rec = await extract_listing_details(url)
                if rec and rec.get('vehicle'):
                    rows.append(rec)
            except Exception as exc:
                print(f" ❌ Error: {exc}")
            
            # Rate limiting
            if i < len(urls_to_process):
                await asyncio.sleep(1.5)
        
        print(f"\n✅ Page {current_page} complete: {len([r for r in rows if r])} total listings")
        
        pages_processed += 1
        current_page += 1
        
        # Update search URL for next iteration
        if pages_processed < batch_pages:
            if next_url:
                search_url = next_url
            else:
                # Construct next page URL manually
                search_url = (
                    f"https://www.pistonheads.com/buy/search"
                    f"?price=0&price=2000&seller-type=Trade"
                    f"&page={current_page}"
                )
            
            # Rate limiting between pages
            await asyncio.sleep(2)
    
    print(f"\n✅ PistonHeads scraping complete: {len(rows)} listings collected")
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
        demo_rows = asyncio