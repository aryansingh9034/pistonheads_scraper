# run_all.py — Enhanced orchestrator with progress tracking and better error handling
import asyncio
import json
from datetime import datetime
from pathlib import Path
from db_helper import save_rows, get_stats
from scrapers.pistonheads_scraper import run_pistonheads
from scrapers.aa_scraper import run_aa

# Progress file to track scraping state
PROGRESS_FILE = "scraping_progress.json"

# ────────────────────────────────────────────────────────────
# Progress tracking functions
# ────────────────────────────────────────────────────────────
def load_progress() -> dict:
    """Load progress from file"""
    if Path(PROGRESS_FILE).exists():
        try:
            with open(PROGRESS_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    
    # Default progress
    return {
        "pistonheads": {
            "last_page": 0,
            "total_scraped": 0,
            "last_run": None
        },
        "aa": {
            "last_page": 0,
            "total_scraped": 0,
            "last_run": None
        }
    }

def save_progress(progress: dict):
    """Save progress to file"""
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)

# ────────────────────────────────────────────────────────────
# Helper: flatten nested {"vehicle":{}, "dealer":{}} → single dict
# ────────────────────────────────────────────────────────────
def flatten(rec: dict) -> dict:
    """Flatten nested vehicle and dealer data into single dict"""
    v = rec.get("vehicle", {})
    d = rec.get("dealer", {})

    flat = {
        "listing_url": rec.get("listing_url"),
        "title": v.get("title"),
        "make": v.get("make"),
        "model": v.get("model"),
        "variant": v.get("variant"),
        "year": v.get("year"),
        "price": v.get("price"),
        "mileage": v.get("mileage"),
        "fuel_type": v.get("fuel_type"),
        "body_type": v.get("body_type"),
        "gearbox": v.get("gearbox"),
        "dealer_name": d.get("name"),
        "dealer_phone": d.get("phone"),
        "dealer_location": d.get("location"),
        "dealer_city": d.get("city"),
    }
    
    # Remove None values
    return {k: v for k, v in flat.items() if v not in (None, "", {})}

# ────────────────────────────────────────────────────────────
# Main orchestrator function
# ────────────────────────────────────────────────────────────
async def main():
    print("="*60)
    print("🚗 AUTO TRADER SCRAPER ORCHESTRATOR")
    print("="*60)
    
    # Load progress
    progress = load_progress()
    
    # Configuration
    config = {
        "pistonheads": {
            "enabled": True,
            "batch_pages": 5,  # Number of pages to scrape
            "max_per_page": 50,  # Max listings per page
            "table": "raw_pistonheads_db"
        },
        "aa": {
            "enabled": True,
            "batch_size": 100,  # Number of listings to scrape
            "table": "raw_aa_db"
        }
    }
    
    all_rows = []
    
    # ─── PISTONHEADS SCRAPER ───
    if config["pistonheads"]["enabled"]:
        print(f"\n{'─'*50}")
        print("🏁 PISTONHEADS SCRAPER")
        print(f"{'─'*50}")
        
        start_page = progress["pistonheads"]["last_page"] + 1
        print(f"📄 Starting from page {start_page}")
        
        try:
            ph_rows = await run_pistonheads(
                batch_pages=config["pistonheads"]["batch_pages"],
                start_page=start_page,
                max_per_page=config["pistonheads"]["max_per_page"]
            )
            
            if ph_rows:
                # Flatten and save to database
                ph_flat = [flatten(r) for r in ph_rows if r]
                save_rows(config["pistonheads"]["table"], ph_flat)
                
                # Update progress
                progress["pistonheads"]["last_page"] = start_page + config["pistonheads"]["batch_pages"] - 1
                progress["pistonheads"]["total_scraped"] += len(ph_flat)
                progress["pistonheads"]["last_run"] = datetime.now().isoformat()
                
                all_rows.extend(ph_flat)
                print(f"✅ PistonHeads: {len(ph_flat)} new listings saved")
            else:
                print("⚠️ No PistonHeads listings found")
                
        except Exception as e:
            print(f"❌ PistonHeads error: {e}")
    
    # ─── AA SCRAPER ───
    if config["aa"]["enabled"]:
        print(f"\n{'─'*50}")
        print("🚗 AA CARS SCRAPER")
        print(f"{'─'*50}")
        
        try:
            # AA scraper handles its own pagination internally
            aa_rows = await run_aa(batch_size=config["aa"]["batch_size"])
            
            if aa_rows:
                # Flatten and save to database
                aa_flat = [flatten(r) for r in aa_rows if r]
                save_rows(config["aa"]["table"], aa_flat)
                
                # Update progress
                progress["aa"]["total_scraped"] += len(aa_flat)
                progress["aa"]["last_run"] = datetime.now().isoformat()
                
                all_rows.extend(aa_flat)
                print(f"✅ AA Cars: {len(aa_flat)} new listings saved")
            else:
                print("⚠️ No AA listings found")
                
        except Exception as e:
            print(f"❌ AA Cars error: {e}")
    
    # Save progress
    save_progress(progress)
    
    # ─── SUMMARY ───
    print(f"\n{'='*60}")
    print("📊 SCRAPING SUMMARY")
    print(f"{'='*60}")
    print(f"✅ Total new listings collected: {len(all_rows)}")
    print(f"   - PistonHeads total: {progress['pistonheads']['total_scraped']}")
    print(f"   - AA Cars total: {progress['aa']['total_scraped']}")
    
    # Get database statistics
    if config["pistonheads"]["enabled"]:
        ph_stats = get_stats(config["pistonheads"]["table"])
        if ph_stats.get('total'):
            print(f"\n📈 PistonHeads Database Stats:")
            print(f"   - Total records: {ph_stats['total']}")
            if ph_stats.get('top_makes'):
                print("   - Top makes:")
                for make, count in ph_stats['top_makes'][:5]:
                    print(f"     • {make}: {count}")
    
    if config["aa"]["enabled"]:
        aa_stats = get_stats(config["aa"]["table"])
        if aa_stats.get('total'):
            print(f"\n📈 AA Cars Database Stats:")
            print(f"   - Total records: {aa_stats['total']}")
            if aa_stats.get('top_makes'):
                print("   - Top makes:")
                for make, count in aa_stats['top_makes'][:5]:
                    print(f"     • {make}: {count}")
    
    print(f"\n{'='*60}")
    print("✅ Scraping complete!")
    print(f"📝 Progress saved to: {PROGRESS_FILE}")
    print(f"{'='*60}")

# ────────────────────────────────────────────────────────────
# Entry point
# ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️ Scraping interrupted by user")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()