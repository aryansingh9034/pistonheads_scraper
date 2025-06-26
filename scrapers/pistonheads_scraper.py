
import asyncio, re, os
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
"""

# ──────────────────────────────────────────────────────────────
#  PAGE‑LEVEL HELPERS
# ──────────────────────────────────────────────────────────────
async def _fetch_html(url: str) -> str:
    async with AsyncWebCrawler(verbose=False, headless=True) as crawler:
        res = await crawler.arun(url=url, timeout=45000, js_code=FETCH_JS)
    return res.html if res.success else ""

async def extract_listings_and_next_page(search_url: str) -> Tuple[List[str], Optional[str]]:
    """Return listing URLs on page + constructed next‑page URL"""
    html = await _fetch_html(search_url)
    if not html:
        return [], None
    soup = BeautifulSoup(html, "html.parser")
    urls: List[str] = []
    for a in soup.find_all("a", href=True):
        h = a["href"]
        if "/buy/listing/" in h and "thumb" not in h:
            full = h if h.startswith("http") else f"https://www.pistonheads.com{h}"
            urls.append(full.split("?", 1)[0])
    # build next page url
    p = urlparse(search_url); qs = parse_qs(p.query); cur = int(qs.get("page", ["1"])[0])
    qs["page"] = [str(cur + 1)]
    next_url = f"{p.scheme}://{p.netloc}{p.path}?{urlencode(qs, doseq=True)}"
    return list(dict.fromkeys(urls)), next_url


async def extract_listing_details(listing_url: str) -> Dict:
    print(f"  🚗 Processing: {listing_url}")
    async with AsyncWebCrawler(verbose=False, headless=True) as crawler:
        result = await crawler.arun(url=listing_url, timeout=30000, js_code=FETCH_JS)
    if not result.success:
        print("  ❌ Failed to fetch listing")
        return {}

    soup = BeautifulSoup(result.html, 'html.parser')
    data = {"listing_url": listing_url, "vehicle": {}, "dealer": {}}

    # ===== VEHICLE DETAILS =====
    title_elem = soup.find('h1')
    if title_elem:
        title = title_elem.get_text(strip=True)
        data['vehicle']['title'] = title
        year_match = re.search(r"\b(19|20)\d{2}\b", title)
        if year_match:
            data['vehicle']['year'] = year_match.group()
            title_clean = title.replace(year_match.group(), '').strip()
        else:
            title_clean = title
        parts = title_clean.split()
        if len(parts) >= 1:
            data['vehicle']['make'] = parts[0]
        if len(parts) >= 2:
            data['vehicle']['model'] = parts[1]
        if len(parts) >= 3:
            data['vehicle']['variant'] = ' '.join(parts[2:])

    price_elem = soup.find(string=re.compile(r'£[\d,]+'))
    if price_elem and (pm := re.search(r'£([\d,]+)', price_elem)):
        data['vehicle']['price'] = f"£{pm.group(1)}"

    # gather spec text
    spec_sections = soup.find_all(['ul', 'div'], class_=re.compile('specs|details|features|key-info'))
    spec_text = ' '.join(sec.get_text(' ', strip=True).lower() for sec in spec_sections)
    spec_text += ' ' + ' '.join(li.get_text(strip=True).lower() for li in soup.find_all('li'))

    # mileage
    for pat in [r'([\d,]+)\s*miles', r'mileage[:\s]*([\d,]+)', r'([\d,]+)\s*mi\b']:
        if (m := re.search(pat, spec_text)):
            data['vehicle']['mileage'] = m.group(1).replace(',', '')
            break
    # fuel
    for fuel in ['petrol', 'diesel', 'electric', 'hybrid', 'plug-in hybrid', 'lpg']:
        if fuel in spec_text:
            data['vehicle']['fuel_type'] = fuel.title(); break
    # gearbox
    for gb in ['manual', 'automatic', 'semi-automatic', 'cvt', 'dsg']:
        if gb in spec_text:
            data['vehicle']['gearbox'] = gb.capitalize(); break
    # body type
    for body in ['hatchback','saloon','estate','suv','coupe','convertible','mpv','pickup','van','4x4','sports','cabriolet']:
        if body in spec_text:
            data['vehicle']['body_type'] = body.title(); break

    # ===== DEALER DETAILS =====
    seller_div = soup.find('div', class_=re.compile('SellerDetails_tradeSellerWrapper'))
    if seller_div and (h3 := seller_div.find('h3')):
        data['dealer']['name'] = h3.get_text(strip=True)
    if (tel := soup.find('a', href=re.compile(r'tel:'))):
        data['dealer']['phone'] = tel['href'].split(':',1)[1]
    if (loc := soup.find(string=re.compile(r',\s*United Kingdom'))):
        loc_txt = loc.strip()
        data['dealer']['location'] = loc_txt
        data['dealer']['city'] = loc_txt.split(',',1)[0]

    return data if data['vehicle'] else {}

# ──────────────────────────────────────────────────────────────
#  PUBLIC BATCH RUNNER (for orchestrator)
# ──────────────────────────────────────────────────────────────
async def run_pistonheads(batch_pages: int = 5, start_page: int = 1, max_per_page: int = 60) -> List[Dict]:
    rows: List[Dict] = []
    page = start_page
    for _ in range(batch_pages):
        search = (
            "https://www.pistonheads.com/buy/search"
            "?price=0&price=2000&seller-type=Trade"
            f"&page={page}"
        )
        listing_urls, _ = await extract_listings_and_next_page(search)
        for url in listing_urls[:max_per_page]:
            try:
                rec = await extract_listing_details(url)
                if rec:
                    rows.append(rec)
            except Exception as exc:
                print(f"⚠️ {url} :: {exc}")
            await asyncio.sleep(1)
        page += 1
    return rows

# ──────────────────────────────────────────────────────────────
#  CLI TEST
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import json, sys
    if len(sys.argv) > 1 and sys.argv[1].startswith("http"):
        targets = sys.argv[1:]
        async def _single(urls):
            return [await extract_listing_details(u) for u in urls]
        print(json.dumps(asyncio.run(_single(targets)), indent=2))
    else:
        demo_rows = asyncio.run(run_pistonheads(batch_pages=1))
        print(json.dumps(demo_rows[:2], indent=2), f"\n… {len(demo_rows)} rows …")
