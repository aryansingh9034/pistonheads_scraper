"""aa_scraper.py – self‑contained
• If `urls` is passed → scrape those detail pages.
• Else → auto‑harvest all “/cardetails/” links from `DEFAULT_SEARCH_URL` up to
  `max_pages` and then scrape them.  No external aa_urls.txt file required.
• Exposes `run_aa()` for orchestrator. Stand‑alone CLI still works.
"""

import asyncio, re, json, os, sys
from typing import List, Dict, Optional, Tuple
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup

FETCH_JS = """
    await new Promise(r => setTimeout(r, 2500));
    window.scrollTo(0, document.body.scrollHeight);
    await new Promise(r => setTimeout(r, 1500));
"""

# Change this to whatever AA search you like
DEFAULT_SEARCH_URL = "https://www.theaa.com/used-cars/search?price-to=2000&page=1"
MAX_PAGES_HARVEST  = 40   # how many search pages to walk when urls=None

# ──────────────────────────────────────────────────────────────
#  HARVESTER – collect /cardetails/ links from AA search pages
# ──────────────────────────────────────────────────────────────
async def _harvest_links(search_url: str, max_pages: int) -> List[str]:
    links, page = [], 1
    next_url = search_url
    while next_url and page <= max_pages:
        async with AsyncWebCrawler(verbose=False, headless=True) as crawler:
            res = await crawler.arun(url=next_url, timeout=40000, js_code=FETCH_JS)
        if not res.success:
            break
        soup = BeautifulSoup(res.html, "html.parser")
        for a in soup.find_all("a", href=True):
            h = a["href"]
            if "/cardetails/" in h:
                full = h if h.startswith("http") else f"https://www.theaa.com{h}"
                links.append(full.split("?",1)[0])
        # AA pagination uses ?page=n in same search URL
        m = re.search(r"[?&]page=(\d+)", next_url)
        nxt = int(m.group(1)) + 1 if m else page + 1
        next_url = re.sub(r"[?&]page=\d+", f"?page={nxt}", search_url, 1)
        page += 1
    return list(dict.fromkeys(links))

# ──────────────────────────────────────────────────────────────
#  SINGLE LISTING PARSER (unchanged core logic)
# ──────────────────────────────────────────────────────────────
async def extract_aa_listing(url: str) -> Dict:
    async with AsyncWebCrawler(verbose=False, headless=True) as crawler:
        res = await crawler.arun(url=url, timeout=40000, js_code=FETCH_JS)
    if not res.success:
        print(f"❌ Fetch failed {url}")
        return {}

    soup = BeautifulSoup(res.html, "html.parser")
    row: Dict = {"listing_url": url, "vehicle": {}, "dealer": {}}

    if (h1 := soup.find("h1")):
        title = h1.get_text(strip=True)
        row["vehicle"]["title"] = title
        parts = title.split()
        if parts: row["vehicle"]["make"]  = parts[0]
        if len(parts) > 1: row["vehicle"]["model"] = parts[1]
        if len(parts) > 2: row["vehicle"]["variant"] = " ".join(parts[2:])

    if (pe := soup.find(string=re.compile(r"£[\d,]+"))) and (pm := re.search(r"£([\d,]+)", pe)):
        row["vehicle"]["price"] = f"£{pm.group(1)}"

    for li in soup.select(".specs-panel li"):
        lab = li.find("span", class_="vd-spec-label")
        val = li.find("span", class_="vd-spec-value")
        if not (lab and val):
            continue
        k, v = lab.get_text(strip=True).lower(), val.get_text(strip=True)
        if "mileage" in k:
            row["vehicle"]["mileage"] = v.replace(",", "").replace(" miles", "")
        elif "year" in k:
            row["vehicle"]["year"] = v
        elif "fuel" in k:
            row["vehicle"]["fuel_type"] = v
        elif "transmission" in k:
            row["vehicle"]["gearbox"] = v
        elif "body type" in k:
            row["vehicle"]["body_type"] = v

    if (s := soup.find("script", string=re.compile("var utag_data"))):
        if (m := re.search(r"utag_data\s*=\s*(\{.*?\});", s.string or "", re.S)):
            try:
                utag = json.loads(m.group(1))
                row["dealer"]["name"] = utag.get("dealerName")
                for fld in ["make","model","vehicleVariant"]:
                    if utag.get(fld):
                        row["vehicle"].setdefault(fld if fld!="vehicleVariant" else "variant", utag[fld])
            except Exception: pass

    if (tel := soup.find("a", href=re.compile(r"tel:"))):
        row["dealer"]["phone"] = tel["href"].split(":",1)[1]

    return row if row["vehicle"] else {}

# ──────────────────────────────────────────────────────────────
#  PUBLIC BATCH RUNNER
# ──────────────────────────────────────────────────────────────
async def run_aa(urls: Optional[List[str]] = None, batch_size: int = 400) -> List[Dict]:
    if urls is None:
        print("🔍 Harvesting AA links …")
        urls = await _harvest_links(DEFAULT_SEARCH_URL, MAX_PAGES_HARVEST)
        print(f"✅ Harvested {len(urls)} detail URLs")
    rows: List[Dict] = []
    for u in urls[:batch_size]:
        try:
            rec = await extract_aa_listing(u)
            if rec:
                rows.append(rec)
        except Exception as e:
            print(f"⚠️ {u} :: {e}")
        await asyncio.sleep(1)
    return rows

# ──────────────────────────────────────────────────────────────
#  CLI TEST
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].startswith("http"):
        test_urls = sys.argv[1:]
        print(json.dumps(asyncio.run(run_aa(test_urls, batch_size=len(test_urls))), indent=2))
    else:
        data = asyncio.run(run_aa(batch_size=5))
        print(json.dumps(data[:2], indent=2), f"\n… {len(data)} rows …")
