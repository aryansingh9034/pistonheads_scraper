"""gumtree_scraper.py - Scraper for Gumtree car listings
• Handles Gumtree's specific HTML structure
• Extracts detailed vehicle and dealer information
• Implements pagination correctly
"""

import asyncio
import re
import json
import sys
import os
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse, parse_qs, urlencode, urljoin
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup
from datetime import datetime
from db_helper import save_to_raw_source

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
    const listings = document.querySelectorAll('a[data-q="search-result-anchor"]');
    console.log('Found listings:', listings.length);
    
    // Return the HTML content
    return document.documentElement.outerHTML;
"""

# Default search URL with max price filter
DEFAULT_SEARCH_URL = "https://www.gumtree.com/search?search_category=cars&search_location=uk&max_price=2000&seller_type=trade&page=1"
MAX_PAGES_HARVEST = 20  # Maximum pages to scrape when no URLs provided

# ──────────────────────────────────────────────────────────────
#  HARVESTER – collect car listing links from Gumtree search pages
# ──────────────────────────────────────────────────────────────
async def _harvest_links(search_url: str, max_pages: int) -> List[str]:
    """Harvest all car listing URLs from Gumtree search pages"""
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
        
        # Look for listing links - Gumtree uses different patterns
        print("  🔍 Searching for car listing links...")
        
        # Method 1: Look for search-result-anchor links (correct method)
        listing_links = soup.select('a[data-q="search-result-anchor"]')
        for link in listing_links:
            href = link.get('href', '')
            if href and '/p/' in href:
                # Gumtree URLs are already full URLs
                if href.startswith('http'):
                    clean_url = href.split('?')[0]
                else:
                    clean_url = f"https://www.gumtree.com{href}".split('?')[0]
                
                if clean_url not in page_links:
                    page_links.append(clean_url)
        
        # Method 2: Look for article links with data-q attribute (fallback)
        if not page_links:
            articles = soup.find_all("article", {"data-q": "search-result"})
            for article in articles:
                link = article.find("a", href=True)
                if link and "/p/" in link["href"]:
                    href = link["href"]
                    full_url = href if href.startswith("http") else f"https://www.gumtree.com{href}"
                    # Remove query parameters
                    clean_url = full_url.split("?", 1)[0].split("#", 1)[0]
                    if clean_url not in page_links:
                        page_links.append(clean_url)
        
        # Method 3: Look for all links with /p/ pattern (final fallback)
        if not page_links:
            for a in soup.find_all("a", href=True):
                href = a["href"]
                # Gumtree uses /p/[make]/[description]/[id] pattern
                if '/p/' in href and re.search(r'/p/[^/]+/[^/]+/\d+', href):
                    full_url = href if href.startswith("http") else f"https://www.gumtree.com{href}"
                    # Remove query parameters
                    clean_url = full_url.split("?", 1)[0].split("#", 1)[0]
                    
                    if clean_url not in page_links and 'gumtree.com' in clean_url:
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
            debug_file = os.path.join(debug_dir, f"gumtree_page_{page}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html")
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
#  SINGLE LISTING PARSER - Extract details from Gumtree listing
# ──────────────────────────────────────────────────────────────
async def extract_gumtree_listing(url: str) -> Dict:
    """Extract detailed information from a single Gumtree listing"""
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
    if "page not found" in page_text.lower() or "404" in page_text:
        print(f"❌ Error page detected for {url}")
        return {}
    
    row: Dict = {"listing_url": url, "vehicle": {}, "dealer": {}}
    v, d = row["vehicle"], row["dealer"]
    
    # 1. Title extraction
    title = None
    title_elem = soup.select_one('h1[itemprop="name"], h1[class*="title"], h1')
    if title_elem:
        title = title_elem.get_text(strip=True)
        v["title"] = title
        
        # Parse title for make/model/year
        # Remove price if present in title
        title_clean = re.sub(r'£[\d,]+', '', title).strip()
        
        # Extract year
        year_match = re.search(r'\b(19[89]\d|20[0-2]\d)\b', title_clean)
        if year_match:
            v["year"] = year_match.group(1)
            title_clean = title_clean.replace(year_match.group(), '').strip()
        
        # Parse make and model
        words = title_clean.split()
        if words:
            v["make"] = words[0]
            if len(words) > 1:
                v["model"] = words[1]
                if len(words) > 2:
                    v["variant"] = ' '.join(words[2:])
    
    # 2. Price extraction
    price_elem = soup.select_one('[itemprop="price"], .h-price, [class*="price"]')
    if price_elem:
        price_text = price_elem.get_text(strip=True)
        price_match = re.search(r'£\s*([\d,]+)', price_text)
        if price_match:
            v["price"] = f"£{price_match.group(1)}"
    
    # 3. Vehicle specifications from attributes list
    # Look for the attributes container
    attrs_container = soup.select_one('[data-q="item-details-tab-content"], .item-details-section, [class*="attribute"]')
    if attrs_container:
        # Find all attribute rows
        attr_rows = attrs_container.select('div[class*="attribute"], li, tr')
        
        for row in attr_rows:
            row_text = row.get_text(strip=True).lower()
            
            # Mileage
            if 'mileage' in row_text:
                mileage_match = re.search(r'(\d{1,3}(?:,\d{3})*)\s*miles?', row_text)
                if mileage_match:
                    v["mileage"] = mileage_match.group(1).replace(',', '')
            
            # Fuel type
            elif 'fuel' in row_text:
                for fuel in ['petrol', 'diesel', 'electric', 'hybrid', 'plug-in hybrid']:
                    if fuel in row_text:
                        v["fuel_type"] = fuel.title()
                        break
            
            # Transmission/Gearbox
            elif 'transmission' in row_text or 'gearbox' in row_text:
                for trans in ['manual', 'automatic', 'semi-automatic']:
                    if trans in row_text:
                        v["gearbox"] = trans.capitalize()
                        break
            
            # Body type
            elif 'body' in row_text:
                for body in ['hatchback', 'saloon', 'estate', 'suv', 'mpv', 'coupe', 'convertible']:
                    if body in row_text:
                        v["body_type"] = body.capitalize()
                        break
    
    # 4. Alternative method: Look for structured data
    structured_data = soup.find('script', type='application/ld+json')
    if structured_data:
        try:
            data = json.loads(structured_data.string)
            if isinstance(data, dict):
                # Extract vehicle details from structured data
                if 'name' in data and not v.get('title'):
                    v['title'] = data['name']
                
                if 'offers' in data and isinstance(data['offers'], dict):
                    if 'price' in data['offers'] and not v.get('price'):
                        v['price'] = f"£{data['offers']['price']}"
                
                # Check for additional vehicle properties
                if 'vehicleEngine' in data:
                    engine_info = data['vehicleEngine']
                    if isinstance(engine_info, dict) and 'fuelType' in engine_info:
                        v['fuel_type'] = engine_info['fuelType']
                
                if 'vehicleTransmission' in data:
                    v['gearbox'] = data['vehicleTransmission']
                
                if 'mileageFromOdometer' in data:
                    if isinstance(data['mileageFromOdometer'], dict):
                        v['mileage'] = str(data['mileageFromOdometer'].get('value', ''))
        except:
            pass
    
    # 5. Dealer information extraction
    # Look for the business seller information section
    seller_section = soup.select_one('.x-business-seller-information, [class*="seller-info"], .ux-section-module__container')
    
    if seller_section:
        # Extract dealer name
        dealer_name_elem = seller_section.select_one('h2, h3, .lightbox-dialog__title, [class*="seller-name"]')
        if dealer_name_elem:
            d["name"] = dealer_name_elem.get_text(strip=True)
        
        # Extract contact details from the content sections
        content_sections = seller_section.select('.ux-section__content, .ux-section-content')
        
        for section in content_sections:
            section_text = section.get_text(strip=True)
            
            # Address extraction
            if 'address' in section_text.lower() or any(postcode in section_text for postcode in ['SW1', 'NW1', 'E1', 'W1']):
                # Look for address components
                address_parts = []
                lines = section.get_text().strip().split('\n')
                for line in lines:
                    line = line.strip()
                    if line and not any(skip in line.lower() for skip in ['address:', 'phone:', 'email:', 'website:']):
                        address_parts.append(line)
                
                if address_parts:
                    d["address"] = ', '.join(address_parts)
                    # Try to extract city (usually second to last part)
                    if len(address_parts) >= 2:
                        d["city"] = address_parts[-2]
            
            # Phone extraction
            phone_match = re.search(r'(?:phone|tel)[:\s]*([\d\s\-\+\(\)]+)', section_text, re.I)
            if phone_match:
                d["phone"] = phone_match.group(1).strip()
            
            # Email extraction
            email_match = re.search(r'(?:email)[:\s]*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', section_text, re.I)
            if email_match:
                d["email"] = email_match.group(1)
        
        # Website extraction - look for links
        website_link = seller_section.select_one('a[href*="http"]:not([href*="gumtree"])')
        if website_link:
            d["website"] = website_link['href']
    
    # Alternative: Look for dealer info in a modal or overlay
    dealer_modal = soup.select_one('.lightbox-dialog, [role="dialog"]')
    if dealer_modal and not d.get("name"):
        # Extract from modal
        name_elem = dealer_modal.select_one('.lightbox-dialog__title, h2, h3')
        if name_elem:
            d["name"] = name_elem.get_text(strip=True)
        
        # Look for contact details in modal
        modal_text = dealer_modal.get_text()
        
        # Phone in modal
        phone_match = re.search(r'(?:phone|tel)[:\s]*([\d\s\-\+\(\)]+)', modal_text, re.I)
        if phone_match:
            d["phone"] = phone_match.group(1).strip()
        
        # Email in modal
        email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', modal_text)
        if email_match:
            d["email"] = email_match.group(1)
    
    # Extract contact form URL if available
    contact_form = soup.select_one('a[href*="contact"], a[href*="enquiry"], button[data-q*="contact"]')
    if contact_form and contact_form.get('href'):
        d["contact_form_url"] = urljoin(url, contact_form['href'])
    
    # Clean up phone number
    if d.get("phone"):
        # Remove any non-digit characters except + and spaces
        phone = re.sub(r'[^\d\s+]', '', d["phone"])
        d["phone"] = phone.strip()
    
    # Ensure we have the listing URL
    row["listing_url"] = url
    
    # Only return if we have meaningful data
    if v.get("title") or v.get("make") or v.get("price"):
        print(f"✅ Extracted: {v.get('make', 'Unknown')} {v.get('model', '')} {v.get('year', '')} - {v.get('price', 'N/A')} from {d.get('name', 'Unknown dealer')}")
        return row
    else:
        print(f"⚠️ No valid vehicle data found for {url}")
        return {}

# ──────────────────────────────────────────────────────────────
#  PUBLIC BATCH RUNNER with pagination support
# ──────────────────────────────────────────────────────────────
async def run_gumtree(
    urls: Optional[List[str]] = None, 
    batch_size: int = 100,
    batch_pages: int = 3,
    start_page: int = 1,
    max_per_page: int = 50
) -> List[Dict]:
    """
    Main entry point for Gumtree scraper
    - If urls provided: scrape those specific listings
    - Otherwise: harvest from search pages and scrape
    - Supports pagination with start_page parameter
    """
    if urls is None:
        print("🔍 Starting Gumtree harvest...")
        print(f"📄 Pages: {start_page} to {start_page + batch_pages - 1}")
        
        # Build search URL with filters
        base_url = "https://www.gumtree.com/search"
        params = {
            "search_category": "cars",
            "search_location": "uk",
            "max_price": "2000",
            "seller_type": "trade",
            "page": str(start_page)
        }
        
        # Harvest the specified pages
        urls = []
        for page_num in range(start_page, start_page + batch_pages):
            params["page"] = str(page_num)
            page_url = f"{base_url}?{urlencode(params)}"
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
        if '/p/' in url and re.search(r'/p/[^/]+/[^/]+/\d+', url):  # Has correct Gumtree pattern
            car_listing_urls.append(url)
        else:
            print(f"  ⏭️ Skipping non-car listing: {url}")
    
    print(f"\n🚗 Scraping {len(car_listing_urls)} actual car listings...")
    
    rows: List[Dict] = []
    batch_save_size = 10  # Save every 10 records
    
    for i, url in enumerate(car_listing_urls, 1):
        try:
            print(f"[{i}/{len(car_listing_urls)}] ", end="")
            rec = await extract_gumtree_listing(url)
            if rec and rec.get("vehicle"):
                rows.append(rec)
                # Save in batches to raw_source
                if len(rows) >= batch_save_size:
                    save_to_raw_source(rows, 'gumtree')
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
        save_to_raw_source(rows, 'gumtree')
    
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
        results = asyncio.run(run_gumtree(test_urls, batch_size=len(test_urls)))
        print(json.dumps(results, indent=2))
    else:
        # Test harvest and scrape just ONE listing
        print("🧪 TESTING GUMTREE SCRAPER - ONE LISTING ONLY")
        print("="*60)
        
        data = asyncio.run(run_gumtree(
            batch_size=1,  # Just one listing
            batch_pages=1,
            start_page=1
        ))
        
        if data:
            print("\n📋 EXTRACTED DATA STRUCTURE:")
            print("="*60)
            listing = data[0]
            print(json.dumps(listing, indent=2, default=str))
            
            print("\n🔍 BREAKDOWN:")
            print("="*60)
            print(f"📄 Listing URL: {listing.get('listing_url', 'N/A')}")
            
            vehicle = listing.get('vehicle', {})
            print(f"\n🚗 VEHICLE DETAILS:")
            print(f"   Title: {vehicle.get('title', 'N/A')}")
            print(f"   Make: {vehicle.get('make', 'N/A')}")
            print(f"   Model: {vehicle.get('model', 'N/A')}")
            print(f"   Year: {vehicle.get('year', 'N/A')}")
            print(f"   Price: {vehicle.get('price', 'N/A')}")
            print(f"   Mileage: {vehicle.get('mileage', 'N/A')}")
            print(f"   Fuel Type: {vehicle.get('fuel_type', 'N/A')}")
            print(f"   Body Type: {vehicle.get('body_type', 'N/A')}")
            print(f"   Gearbox: {vehicle.get('gearbox', 'N/A')}")
            
            dealer = listing.get('dealer', {})
            print(f"\n🏢 DEALER/COMPANY DETAILS:")
            print(f"   Name: {dealer.get('name', 'N/A')}")
            print(f"   Phone: {dealer.get('phone', 'N/A')}")
            print(f"   Email: {dealer.get('email', 'N/A')}")
            print(f"   Address: {dealer.get('address', 'N/A')}")
            print(f"   City: {dealer.get('city', 'N/A')}")
            print(f"   Website: {dealer.get('website', 'N/A')}")
            print(f"   Contact Form URL: {dealer.get('contact_form_url', 'N/A')}")
            
            print(f"\n✅ Total: {len(data)} listing extracted")
        else:
            print("❌ No data collected")