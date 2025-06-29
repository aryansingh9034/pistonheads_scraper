#!/usr/bin/env python3
"""
Test script to verify pagination is working correctly
"""

import asyncio
import json
from pathlib import Path
from run_all import load_progress, save_progress

async def test_pagination():
    """Test that pagination tracking works correctly"""
    
    print("🧪 TESTING PAGINATION SYSTEM")
    print("="*50)
    
    # Load current progress
    progress = load_progress()
    
    print("📊 CURRENT PROGRESS:")
    for scraper in ['pistonheads', 'aa', 'cazoo']:
        last_page = progress[scraper]['last_page']
        total_listings = progress[scraper]['total_listings']
        print(f"   - {scraper.title()}: Page {last_page}, {total_listings} listings")
    
    print("\n🔄 SIMULATING NEXT RUN:")
    for scraper in ['pistonheads', 'aa', 'cazoo']:
        next_page = progress[scraper]['last_page'] + 1
        print(f"   - {scraper.title()}: Will start from page {next_page}")
    
    # Simulate running 3 pages for each scraper
    print("\n📈 SIMULATING RUNNING 3 PAGES EACH:")
    for scraper in ['pistonheads', 'aa', 'cazoo']:
        current_page = progress[scraper]['last_page']
        new_page = current_page + 3
        progress[scraper]['last_page'] = new_page
        progress[scraper]['total_pages_scraped'] += 3
        progress[scraper]['total_listings'] += 50  # Simulate 50 listings per run
        print(f"   - {scraper.title()}: {current_page} → {new_page} (+50 listings)")
    
    # Save updated progress
    save_progress(progress)
    
    print("\n✅ PAGINATION TEST COMPLETE")
    print("📄 Progress saved to scraping_progress.json")
    
    # Show final state
    print("\n📊 FINAL PROGRESS:")
    for scraper in ['pistonheads', 'aa', 'cazoo']:
        last_page = progress[scraper]['last_page']
        total_listings = progress[scraper]['total_listings']
        print(f"   - {scraper.title()}: Page {last_page}, {total_listings} listings")

if __name__ == "__main__":
    asyncio.run(test_pagination()) 