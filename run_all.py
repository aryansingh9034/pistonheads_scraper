# helper to squash vehicle + dealer into one flat dict
def flatten(rec: dict) -> dict:
    flat = {
        "listing_url": rec.get("listing_url"),
        # vehicle fields
        **rec.get("vehicle", {}),
        # dealer fields
        **rec.get("dealer", {})
    }
    # drop keys your table doesn’t have
    flat.pop("variant",   None)
    flat.pop("body_type", None)
    return flat

async def main():
    aa_data        = await run_aa()
    piston_data    = await run_pistonheads()

    all_rows_flat  = [flatten(r) for r in (aa_data + piston_data)]

    # store into the table that actually exists
    save_rows("raw_pistonheads_db", all_rows_flat)
