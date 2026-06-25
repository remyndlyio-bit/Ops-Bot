"""
Seeded synthetic dataset for the golden source.

Deterministic (seeded RNG) so the corpus + every computed answer are reproducible.
Models the real quirks the AI must learn: brands that differ from the billing
client (Pepsi → "Content Lab"), messy `paid` (Yes / blank / null), POCs with and
without email, a spread of dates and fee sizes.

The anonymized-real loader (Phase 2) will return rows in this SAME shape, so the
oracle / generator / retriever are source-agnostic.
"""
import random
from datetime import date, timedelta

# (billing client_name, [brands], production_house, poc_name|None, email_domain|None)
CLIENTS = [
    ("Star Studios", ["Nike", "Adidas", "Puma"], "Star Studios", "Karan", "starstudios.com"),
    ("Garnier India", ["Garnier", "Garnier Men", "L'Oreal"], "Garnier India", "Priya", "garnier.com"),
    ("Samsung India", ["Samsung", "Galaxy"], "Samsung India", "Rahul", "samsung.com"),
    ("Maruti Suzuki", ["Maruti", "Swift", "Brezza"], "Maruti Suzuki", "Anita", "maruti.co.in"),
    ("Pedigree Films", ["Pedigree", "Whiskas"], "Pedigree Films", None, None),
    ("Content Lab", ["Pepsi", "Lays", "Tropicana"], "Content Lab", "Sam", "pepsi.com"),
    ("Ogilvy Mumbai", ["Cadbury", "Bournvita"], "Ogilvy Mumbai", "Neha", "ogilvy.com"),
    ("Dentsu Webchutney", ["Swiggy", "Zomato"], "Dentsu Webchutney", "Vikram", None),
    ("Famous Innovations", ["Royal Enfield", "Bajaj"], "Famous Innovations", "Meera", "famous.in"),
    ("Lowe Lintas", ["Surf Excel", "Lifebuoy"], "Lowe Lintas", "Arjun", "lintas.com"),
]
DESCS = [
    "TVC 30sec + cutdowns", "Brand film 60sec", "Product shoot", "Social media cutdowns",
    "Print campaign", "Digital launch film", "Radio spot", "Packshot photography",
    "Influencer reels", "Corporate AV", "Festival campaign", "Hindi dubbing",
]
FEE_BUCKETS = [25000, 40000, 55000, 75000, 90000, 120000, 150000, 200000, 300000, 450000]


def build_dataset(seed: int = 42, n: int = 120):
    """Return n deterministic job_entries-shaped rows."""
    rng = random.Random(seed)
    start = date(2025, 1, 1)
    span = (date(2026, 6, 30) - start).days
    rows = []
    for i in range(1, n + 1):
        client, brands, ph, poc, domain = CLIENTS[rng.randrange(len(CLIENTS))]
        brand = brands[rng.randrange(len(brands))]
        jd = start + timedelta(days=rng.randint(0, span))
        fees = FEE_BUCKETS[rng.randrange(len(FEE_BUCKETS))]
        paid = "Yes" if rng.random() < 0.55 else None        # ~55% paid; rest null
        bill_sent = "Yes" if (paid == "Yes" or rng.random() < 0.4) else None
        has_email = poc is not None and domain is not None
        rows.append({
            "id": i,
            "user_id": "golden",
            "client_name": client,
            "brand_name": brand,
            "production_house": ph,
            "poc_name": poc,
            "poc_email": (f"{poc.lower()}@{domain}" if has_email else None),
            "job_date": jd.isoformat(),
            "fees": fees,
            "paid": paid,
            "bill_sent": bill_sent,
            "invoice_date": (jd + timedelta(days=5)).isoformat() if bill_sent else None,
            "bill_no": f"INV-{i:04d}",
            "job_description_details": DESCS[rng.randrange(len(DESCS))],
            "isDeleted": None,
        })
    return rows


# Entities present in the default dataset, for the generator to slot in.
def entities():
    return {
        "clients": [c[0] for c in CLIENTS],
        "brands": sorted({b for c in CLIENTS for b in c[1]}),
    }
