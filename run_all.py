# run_all.py — Enhanced orchestrator with page-based progress tracking
import asyncio
import json
from datetime import datetime
from pathlib import Path
from db_helper import save_to_leads, get_stats_by_source
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
            "total_pages_scraped": 0,
            "total_listings": 0,
            "last_run": None
        },
        "aa": {
            "last_page": 0,
            "total_pages_scraped": 0,
            "total_listings": 0,
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
            "pages_per_run": 3,  # Number of pages to scrape per run
            "max_listings_per_page": 50,  # Max listings per page
        },
        "aa": {
            "enabled": True,
            "pages_per_run": 3,  # Number of pages to scrape per run
            "max_listings_per_batch": 100,  # Max listings to process
        }
    }
    
    # Show current progress
    print("\n📊 CURRENT PROGRESS:")
    print(f"   - PistonHeads: Last page {progress['pistonheads']['last_page']}, "
          f"Total scraped: {progress['pistonheads']['total_listings']}")
    print(f"   - AA Cars: Last page {progress['aa']['last_page']}, "
          f"Total scraped: {progress['aa']['total_listings']}")
    
    # ─── PISTONHEADS SCRAPER ───
    if config["pistonheads"]["enabled"]:
        print(f"\n{'─'*50}")
        print("🏁 PISTONHEADS SCRAPER")
        print(f"{'─'*50}")
        
        # Calculate start page
        start_page = progress["pistonheads"]["last_page"] + 1
        end_page = start_page + config["pistonheads"]["pages_per_run"] - 1
        
        print(f"📄 Scraping pages {start_page} to {end_page}")
        
        try:
            ph_rows = await run_pistonheads(
                batch_pages=config["pistonheads"]["pages_per_run"],
                start_page=start_page,
                max_per_page=config["pistonheads"]["max_listings_per_page"]
            )
            
            if ph_rows:
                # Flatten and save to leads table
                ph_flat = [flatten(r) for r in ph_rows if r]
                save_to_leads(ph_flat, "PistonHeads")
                
                # Update progress
                progress["pistonheads"]["last_page"] = end_page
                progress["pistonheads"]["total_pages_scraped"] += config["pistonheads"]["pages_per_run"]
                progress["pistonheads"]["total_listings"] += len(ph_flat)
                progress["pistonheads"]["last_run"] = datetime.now().isoformat()
                
                print(f"✅ PistonHeads: {len(ph_flat)} new listings saved")
                print(f"📄 Next run will start from page {progress['pistonheads']['last_page'] + 1}")
            else:
                print("⚠️ No PistonHeads listings found")
                
        except Exception as e:
            print(f"❌ PistonHeads error: {e}")
            import traceback
            traceback.print_exc()
    
    # ─── AA SCRAPER ───
    if config["aa"]["enabled"]:
        print(f"\n{'─'*50}")
        print("🚗 AA CARS SCRAPER")
        print(f"{'─'*50}")
        
        try:
            # For AA scraper, we need to modify it to accept start_page parameter
            # For now, we'll use the batch_size parameter
            aa_rows = await run_aa(batch_size=config["aa"]["max_listings_per_batch"])
            
            if aa_rows:
                # Flatten and save to leads table
                aa_flat = [flatten(r) for r in aa_rows if r]
                save_to_leads(aa_flat, "AA")
                
                # Update progress (AA scraper handles its own pagination internally)
                # We estimate pages based on listings (assuming ~20 listings per page)
                estimated_pages = len(aa_flat) // 20 + 1
                progress["aa"]["last_page"] += estimated_pages
                progress["aa"]["total_pages_scraped"] += estimated_pages
                progress["aa"]["total_listings"] += len(aa_flat)
                progress["aa"]["last_run"] = datetime.now().isoformat()
                
                print(f"✅ AA Cars: {len(aa_flat)} new listings saved")
                print(f"📄 Estimated {estimated_pages} pages processed")
            else:
                print("⚠️ No AA listings found")
                
        except Exception as e:
            print(f"❌ AA Cars error: {e}")
            import traceback
            traceback.print_exc()
    
    # Save progress
    save_progress(progress)
    
    # ─── SUMMARY ───
    print(f"\n{'='*60}")
    print("📊 SCRAPING SUMMARY")
    print(f"{'='*60}")
    
    # Get database statistics
    stats = get_stats_by_source()
    
    if stats.get('total'):
        print(f"✅ Total records in leads table: {stats['total']}")
        
        if stats.get('by_source'):
            print("\n📈 Records by source:")
            for source, count in stats['by_source'].items():
                print(f"   - {source}: {count}")
        
        if stats.get('last_24h'):
            print("\n⏱️ Last 24 hours:")
            for source, count in stats['last_24h'].items():
                print(f"   - {source}: {count} new records")
    
    print(f"\n📝 Progress saved to: {PROGRESS_FILE}")
    print("\n🔄 NEXT RUN WILL START FROM:")
    print(f"   - PistonHeads: Page {progress['pistonheads']['last_page'] + 1}")
    print(f"   - AA Cars: Will continue from last position")
    print(f"{'='*60}")

# ────────────────────────────────────────────────────────────
# CLI Arguments Support
# ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    
    # Check for command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "--reset":
            # Reset progress
            print("🔄 Resetting progress...")
            save_progress({
                "pistonheads": {
                    "last_page": 0,
                    "total_pages_scraped": 0,
                    "total_listings": 0,
                    "last_run": None
                },
                "aa": {
                    "last_page": 0,
                    "total_pages_scraped": 0,
                    "total_listings": 0,
                    "last_run": None
                }
            })
            print("✅ Progress reset complete")
        elif sys.argv[1] == "--status":
            # Show status only
            progress = load_progress()
            stats = get_stats_by_source()
            
            print("📊 SCRAPER STATUS")
            print("="*50)
            print("\nProgress:")
            print(json.dumps(progress, indent=2))
            print("\nDatabase Stats:")
            print(json.dumps(stats, indent=2))
        else:
            print("Usage:")
            print("  python run_all.py          # Run scrapers")
            print("  python run_all.py --reset  # Reset progress")
            print("  python run_all.py --status # Show status")
    else:
        # Normal run
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            print("\n⚠️ Scraping interrupted by user")
        except Exception as e:
            print(f"\n❌ Fatal error: {e}")
            import traceback
            traceback.print_exc()