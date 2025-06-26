"""aa_scraper.py – Updated with proper pagination and better extraction
• Handles the displaycars URL format
• Properly extracts listing URLs from search results
• Implements pagination correctly
"""

import asyncio, re, json, os, sys
from typing import List, Dict, Optional, Tuple
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urlencode

FETCH_JS = """
    await new Promise(r => setTimeout(r, 3000));
    window.scrollTo(0, document.body.scrollHeight);
    await new Promise(r => setTimeout(r, 2000));
    window.scrollTo(0, 0);
    await new Promise(r => setTimeout(r, 1000));
"""

# Updated default search URL to use displaycars format
DEFAULT_SEARCH_URL = "https://www.theaa.com/used-cars/displaycars?fullpostcode=PR267SY&travel=2000&priceto=2000&page=1"
MAX_PAGES_HARVEST = 40   # how many search pages to walk when urls=None

# ──────────────────────────────────────────────────────────────
#  HARVESTER – collect car listing links from AA search pages
# ──────────────────────────────────────────────────────────────
async def _harvest_links(search_url: str, max_pages: int) -> List[str]:
    """Harvest all car listing URLs from AA search pages"""
    all_links = []
    page = 1
    
    while page <= max_pages:
        print(f"🔍 Harvesting page {page}...")
        
        # Update page number in URL
        parsed = urlparse(search_url)
        params = parse_qs(parsed.query)
        params['page'] = [str(page)]
        current_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{urlencode(params, doseq=True)}"
        
        async with AsyncWebCrawler(verbose=False, headless=True) as crawler:
            res = await crawler.arun(url=current_url, timeout=45000, js_code=FETCH_JS)
        
        if not res.success:
            print(f"❌ Failed to fetch page {page}")
            break
            
        soup = BeautifulSoup(res.html, "html.parser")
        page_links = []
        
        # Method 1: Look for links with specific patterns
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # AA patterns: /cardetails/, /used-cars/cardetails/, or similar
            if any(pattern in href for pattern in ['/cardetails/', '/vehicle/', '/car-details/']):
                full_url = href if href.startswith("http") else f"https://www.theaa.com{href}"
                clean_url = full_url.split("?", 1)[0]
                if clean_url not in page_links:
                    page_links.append(clean_url)
        
        # Method 2: Look for listing cards with specific classes
        listing_cards = soup.select(
            'div[class*="vehicle-card"] a, '
            'div[class*="car-card"] a, '
            'article[class*="listing"] a, '
            'div[class*="search-result"] a'
        )
        
        for card_link in listing_cards:
            href = card_link.get('href', '')
            if href and href != '#':
                full_url = href if href.startswith("http") else f"https://www.theaa.com{href}"
                clean_url = full_url.split("?", 1)[0]
                if clean_url not in page_links and 'theaa.com' in clean_url:
                    page_links.append(clean_url)
        
        if not page_links:
            print(f"⚠️ No listings found on page {page}, stopping harvest")
            break
            
        all_links.extend(page_links)
        print(f"✅ Found {len(page_links)} listings on page {page}")
        
        # Check if there's a next page by looking for pagination or if we found listings
        # If we found listings, assume there might be more pages
        page += 1
        await asyncio.sleep(1)  # Be nice to the server
    
    # Remove duplicates while preserving order
    unique_links = list(dict.fromkeys(all_links))
    return unique_links

# ──────────────────────────────────────────────────────────────
#  SINGLE LISTING PARSER - Enhanced extraction
# ──────────────────────────────────────────────────────────────
async def extract_aa_listing(url: str) -> Dict:
    """Extract detailed information from a single AA listing"""
    async with AsyncWebCrawler(verbose=False, headless=True) as crawler:
        res = await crawler.arun(url=url, timeout=45000, js_code=FETCH_JS)
    
    if not res.success:
        print(f"❌ Fetch failed {url}")
        return {}

    soup = BeautifulSoup(res.html, "html.parser")
    row: Dict = {"listing_url": url, "vehicle": {}, "dealer": {}}
    v, d = row["vehicle"], row["dealer"]

    # 1. Title extraction
    if h1 := soup.find("h1"):
        title = h1.get_text(strip=True)
        v["title"] = title
        
        # Extract year
        if year_match := re.search(r'\b(19|20)\d{2}\b', title):
            v["year"] = year_match.group()
            title_clean = title.replace(year_match.group(), '').strip()
        else:
            title_clean = title
            
        # Extract make, model, variant
        parts = title_clean.split()
        if parts:
            v["make"] = parts[0]
            if len(parts) > 1:
                v["model"] = parts[1]
            if len(parts) > 2:
                v["variant"] = " ".join(parts[2:])

    # 2. Price extraction - multiple methods
    price_selectors = [
        '.vehicle-price',
        '[data-testid="vehicle-price"]',
        '.price',
        'span[class*="price"]',
        'div[class*="price"]'
    ]
    
    for selector in price_selectors:
        if price_elem := soup.select_one(selector):
            price_text = price_elem.get_text(strip=True)
            if price_match := re.search(r'£([\d,]+)', price_text):
                v["price"] = f"£{price_match.group(1)}"
                break
    
    # Also search in text
    if "price" not in v:
        if price_text := soup.find(string=re.compile(r'£[\d,]+')):
            if price_match := re.search(r'£([\d,]+)', str(price_text)):
                v["price"] = f"£{price_match.group(1)}"

    # 3. Specifications - Enhanced extraction
    spec_containers = soup.select(
        '.specs-panel li, '
        '.vd-spec, '
        'li[class*="spec"], '
        'div[class*="specification"] li, '
        'ul[class*="key-facts"] li'
    )
    
    for spec in spec_containers:
        label_elem = spec.find(['span', 'div'], class_=re.compile('label|key'))
        value_elem = spec.find(['span', 'div'], class_=re.compile('value|text'))
        
        if label_elem and value_elem:
            label = label_elem.get_text(strip=True).lower()
            value = value_elem.get_text(strip=True)
            
            if 'mileage' in label:
                v["mileage"] = re.sub(r'[^\d]', '', value)
            elif 'year' in label and 'year' not in v:
                v["year"] = value
            elif 'fuel' in label:
                v["fuel_type"] = value
            elif 'transmission' in label or 'gearbox' in label:
                v["gearbox"] = value
            elif 'body' in label:
                v["body_type"] = value
            elif 'make' in label and 'make' not in v:
                v["make"] = value
            elif 'model' in label and 'model' not in v:
                v["model"] = value

    # 4. utag_data script extraction
    if script := soup.find("script", string=re.compile("var\\s+utag_data")):
        if match := re.search(r"utag_data\s*=\s*({.*?});", script.string or "", re.S):
            try:
                utag = json.loads(match.group(1))
                
                # Vehicle data mapping
                mapping = {
                    "make": "make",
                    "model": "model",
                    "vehicleVariant": "variant",
                    "fuelType": "fuel_type",
                    "bodyType": "body_type",
                    "transmissionType": "gearbox",
                    "mileage": "mileage",
                    "modelYear": "year"
                }
                
                for utag_key, our_key in mapping.items():
                    if utag_value := utag.get(utag_key):
                        v.setdefault(our_key, str(utag_value))
                
                # Dealer info
                if dealer_name := utag.get("dealerName"):
                    d["name"] = dealer_name
                    
            except Exception as e:
                print(f"⚠️ Failed to parse utag_data: {e}")

    # 5. Dealer information extraction
    dealer_selectors = [
        '[data-testid="dealer-name"]',
        '.dealer-name',
        'h2[class*="dealer"]',
        'div[class*="dealer-info"] h3'
    ]
    
    for selector in dealer_selectors:
        if dealer_elem := soup.select_one(selector):
            d.setdefault("name", dealer_elem.get_text(strip=True))
            break

    # Phone number
    if tel := soup.find("a", href=re.compile(r"tel:")):
        d["phone"] = tel["href"].split(":", 1)[1]

    # Location
    location_selectors = [
        '[data-testid="dealer-location"]',
        '.dealer-location',
        'div[class*="location"]'
    ]
    
    for selector in location_selectors:
        if loc_elem := soup.select_one(selector):
            location = loc_elem.get_text(strip=True)
            d["location"] = location
            if ',' in location:
                d["city"] = location.split(',')[0].strip()
            break

    # Clean up mileage if it exists
    if mileage := v.get("mileage"):
        v["mileage"] = re.sub(r'[^\d]', '', str(mileage))

    print(f"✅ Extracted: {v.get('make')} {v.get('model')} {v.get('year')} - £{v.get('price', 'N/A')}")
    
    return row if v else {}

# ──────────────────────────────────────────────────────────────
#  PUBLIC BATCH RUNNER
# ──────────────────────────────────────────────────────────────
async def run_aa(urls: Optional[List[str]] = None, batch_size: int = 400) -> List[Dict]:
    """
    Main entry point for AA scraper
    - If urls provided: scrape those specific listings
    - Otherwise: harvest from search pages and scrape
    """
    if urls is None:
        print("🔍 Starting AA harvest...")
        urls = await _harvest_links(DEFAULT_SEARCH_URL, MAX_PAGES_HARVEST)
        print(f"✅ Harvested {len(urls)} detail URLs")
        
        if not urls:
            print("❌ No URLs harvested!")
            return []
    
    rows: List[Dict] = []
    urls_to_scrape = urls[:batch_size]
    
    print(f"🚗 Scraping {len(urls_to_scrape)} AA listings...")
    
    for i, url in enumerate(urls_to_scrape, 1):
        try:
            print(f"[{i}/{len(urls_to_scrape)}] ", end="")
            rec = await extract_aa_listing(url)
            if rec and rec.get("vehicle"):
                rows.append(rec)
            else:
                print(f"⚠️ No data from {url}")
        except Exception as e:
            print(f"❌ Error on {url}: {e}")
        
        # Rate limiting
        if i < len(urls_to_scrape):
            await asyncio.sleep(1.5)
    
    print(f"\n✅ Successfully scraped {len(rows)} listings")
    return rows

# ──────────────────────────────────────────────────────────────
#  CLI TEST
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].startswith("http"):
        # Test specific URLs
        test_urls = sys.argv[1:]
        results = asyncio.run(run_aa(test_urls, batch_size=len(test_urls)))
        print(json.dumps(results, indent=2))
    else:
        # Test harvest and scrape
        data = asyncio.run(run_aa(batch_size=10))
        if data:
            print(json.dumps(data[:2], indent=2))
            print(f"\n✅ Total: {len(data)} listings")
        else:
            print("❌ No data collected")