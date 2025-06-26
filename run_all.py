from scrapers.aa_scraper import run_aa
from scrapers.pistonheads_scraper import run_pistonheads

import asyncio

async def main():
    print("\n🚗 Running AA scraper")
    aa_data = await run_aa()
    print(f"✅ AA done: {len(aa_data)} listings\n")

    print("\n🏁 Running PistonHeads scraper")
    piston_data = await run_pistonheads()
    print(f"✅ PistonHeads done: {len(piston_data)} listings\n")

    # Save to DB (if using db_helper.save_rows)
    from db_helper import save_rows
    all_rows = aa_data + piston_data          
    save_rows("used_car_leads", all_rows)     

if __name__ == "__main__":
    asyncio.run(main())
