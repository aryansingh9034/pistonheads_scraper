# run_all.py  ─ orchestrator
import asyncio
from db_helper import save_rows
from scrapers.pistonheads_scraper import run_pistonheads
from scrapers.aa_scraper          import run_aa


# ────────────────────────────────────────────────────────────
# helper: flatten nested {"vehicle":{}, "dealer":{}} → single dict
# ────────────────────────────────────────────────────────────
def flatten(rec: dict) -> dict:
    v = rec.get("vehicle", {})
    d = rec.get("dealer",  {})

    flat = {
        "listing_url"   : rec.get("listing_url"),
        "title"         : v.get("title"),
        "make"          : v.get("make"),
        "model"         : v.get("model"),
        "year"          : v.get("year"),
        "price"         : v.get("price"),
        "mileage"       : v.get("mileage"),
        "fuel_type"     : v.get("fuel_type"),
        "gearbox"       : v.get("gearbox"),
        # dealer mapping ↓
        "dealer_name"   : d.get("name"),
        "dealer_phone"  : d.get("phone"),
        "dealer_location": d.get("location"),
        "dealer_city"   : d.get("city"),
    }
    # drop keys the table doesn't have
    return {k: v for k, v in flat.items() if v not in (None, "", {})}


async def main():
    print("🚗 Running PistonHeads scraper …")
    ph_rows = await run_pistonheads(batch_pages=1, start_page=1)

    print("🚗 Running AA scraper …")
    aa_rows = await run_aa(batch_size=50)      # adjust batch_size as you like

    all_flat = [flatten(r) for r in ph_rows + aa_rows]
    print(f"✅ Total rows ready for DB: {len(all_flat)}")

    # one insert for everything
    save_rows("raw_pistonheads_db", all_flat)  # <── table name first
    print("✅ DB insert complete")


if __name__ == "__main__":
    asyncio.run(main())
