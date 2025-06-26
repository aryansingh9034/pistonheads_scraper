# run_all.py  ─ orchestrator
import asyncio
from db_helper import save_rows
from scrapers.pistonheads_scraper import run_pistonheads
from scrapers.aa_scraper          import run_aa


# ────────────────────────────────────────────────────────────
# helper: flatten nested {"vehicle":{}, "dealer":{}} → single dict
# ────────────────────────────────────────────────────────────
def flatten(rec: dict) -> dict:
    flat = {
        "listing_url": rec.get("listing_url")
    }
    flat.update(rec.get("vehicle",  {}))
    flat.update(rec.get("dealer",   {}))

    # strip columns that don’t exist in raw_pistonheads_db
    flat.pop("variant",   None)
    flat.pop("body_type", None)

    return flat


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
