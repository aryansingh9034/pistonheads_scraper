#!/usr/bin/env python3
import asyncio, datetime
from scrapers.pistonheads_scraper import run_pistonheads
from scrapers.aa_scraper          import run_aa
from db_helper import save_rows

TABLE = "used_car_leads"   # single table with source column

async def main():
    t0 = datetime.datetime.utcnow()
    ph_rows, aa_rows = await asyncio.gather(
        run_pistonheads(batch_pages=20),
        run_aa(batch_size=400)             # reads aa_urls.txt by default
    )

    all_rows = []
    for src, rows in (("pistonheads", ph_rows), ("aa", aa_rows)):
        for r in rows:
            flat = {
                "source": src,
                "listing_url": r["listing_url"],
                **r["vehicle"],
                **{f"dealer_{k}": v for k, v in r["dealer"].items()}
            }
            all_rows.append(flat)

    save_rows(TABLE, all_rows)
    print(f"✅ Stored {len(all_rows)} rows into {TABLE}  ({datetime.datetime.utcnow()-t0})")

if __name__ == "__main__":
    asyncio.run(main())
