#!/usr/bin/env python3
"""
Infinity Collectables – Automated Product Tagger v1.0
=====================================================
Connects to Shopify Admin API, reads products, and applies structured tags
using Claude AI based on the Infinity Collectables Tagging Framework v7.0.

Usage:
    python infinity_tagger.py                  # Tag all products
    python infinity_tagger.py --dry-run        # Preview without writing
    python infinity_tagger.py --new-only       # Only tag untagged/new products
    python infinity_tagger.py --product-id ID  # Tag a specific product
    python infinity_tagger.py --batch-size 50  # Custom batch size
"""

import os
import sys
import json
import csv
import time
import argparse
import math
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

# Load .env from the same directory as this script
_script_dir = Path(__file__).resolve().parent
load_dotenv(_script_dir / ".env", override=True)

SHOPIFY_STORE_URL = os.getenv("SHOPIFY_STORE_URL", "").strip().rstrip("/")
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN", "").strip()
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()

# Shopify API version
SHOPIFY_API_VERSION = "2024-10"

# Claude model – Haiku is fast and cheap for structured tagging
CLAUDE_MODEL = "claude-haiku-4-5-20251001"

# Rate limiting
SHOPIFY_RATE_LIMIT_DELAY = 0.5  # seconds between Shopify API calls
CLAUDE_RATE_LIMIT_DELAY = 0.2   # seconds between Claude API calls

# Batch size for processing
DEFAULT_BATCH_SIZE = 50

# Output directory
OUTPUT_DIR = Path(".")
TODAY = datetime.now().strftime("%Y-%m-%d")

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(OUTPUT_DIR / f"tagger_log_{TODAY}.txt"),
    ],
)
log = logging.getLogger("infinity_tagger")

# ---------------------------------------------------------------------------
# TAGGING FRAMEWORK v7.0 – SYSTEM PROMPT
# ---------------------------------------------------------------------------

TAGGING_SYSTEM_PROMPT = """You are a product tagging engine for Infinity Collectables, an online collectables retailer.

You MUST follow the Infinity Collectables Official Shopify Product Tagging Framework v7.0 EXACTLY.

## YOUR TASK
Given a product's title, description, price, and product type from Shopify, output the correct structured tags as a JSON object.

## TAG CATEGORIES (apply in this order)

### A) Brand (Manufacturer)
- Exact official brand name. No abbreviations. No prefixes.
- Examples: Funko, Hasbro, Mattel, Bandai, McFarlane Toys, Loungefly, NECA

### B) Franchise / League / IP (if applicable)
- Official franchise, league, team, or universe name.
- Only include if explicitly referenced or clearly verified.
- IMPORTANT: For sports teams, ALWAYS include BOTH the team name AND their league/competition as separate entries in a "franchises" array.
  - Arsenal FC → ["Arsenal FC", "Premier League"]
  - Manchester United → ["Manchester United", "Premier League"]
  - AC Milan → ["AC Milan", "Serie A"]
  - Real Madrid → ["Real Madrid", "La Liga"]
  - Barcelona → ["Barcelona", "La Liga"]
  - PSG → ["PSG", "Ligue 1"]
  - Bayern Munich → ["Bayern Munich", "Bundesliga"]
  - Argentina (national team) → ["Argentina"]
  - England (national team) → ["England"]
  - NBA teams → include team name + "NBA"
  - NFL teams → include team name + "NFL"
  - If league is unclear → include team name only, flag league for escalation
- Examples (non-sports): Marvel, DC Comics, Star Wars, Dragon Ball Z, Pokemon, Jurassic Park

### C) Character / Player (if applicable)
- Exact official name. Only if product specifically represents them.
- Examples: Spider-Man, Batman, Goku, LeBron James, Pikachu

### D) Product Type (mandatory)
- Singular format. Must reflect the physical product.
- Examples: Action Figure, Plush, Statue, Hoodie, T-Shirt, Cap, Jersey, Backpack, Trading Cards, Replica, Vinyl Figure, Bobblehead, Keychain, Mug, Poster, Board Game, Puzzle, Pin, Patch, Wallet, Tote Bag

### E) Category (mandatory – ONE only)
- Collectable, Toy, Sports, Clothing, Accessory
- Only ONE per product.

### F) Age (controlled inference allowed)
- Priority: manufacturer guidance > brand website > major retailers > comparable product line
- Structured logic if unavailable:
  - Standard vinyl figures → Age 14+
  - Plush → Age 3+ or Age 6+
  - Blind boxes → Age 8+ or Age 14+
  - Statues → Age 14+ or Age 18+
  - Clothing/sports → no age unless specified
- IMPORTANT: Always prefix with "Age ". Allowed formats: Age 3+, Age 6+, Age 8+, Age 12+, Age 14+, Age 18+
- If uncertain → set to null and flag for escalation

### G) Gender (controlled inference + research allowed)
- Allowed values: Male, Female, Unisex
- Priority: manufacturer spec > brand website > major retailer categorisation > product cut/fit
- "Boys" → Male, "Girls" → Female
- If not clearly labelled → Unisex
- No separate Kids tag

### H) Price Band (mandatory)
- Based on the selling price provided. Round to nearest whole £.
- IMPORTANT: Always prefix with "Price ". Output as string. Example: £19.99 → "Price 20", £23.50 → "Price 24"

### I) Size (if applicable)
- Figures: 4 Inch, 6 Inch, 10 Inch
- Clothing: XS, S, M, L, XL, XXL
- Accessories: Mini, Standard, Large
- Only include if clearly defined.

### J) Sport Type (sports products only)
- Football, Basketball, Baseball, Motorsport, Rugby, Tennis
- Only if applicable.

## OPTIONAL TAGS (only if officially designated)
- Exclusive, Limited Edition, Chase, Rare
- No invented scarcity.

## WHAT MUST NOT BE TAGGED
- Stock status, release year, marketing phrases, emotional descriptors
- Seasonal campaigns, duplicate wording, unverified assumptions

## OUTPUT FORMAT
Return ONLY valid JSON in this exact structure:
{
  "brand": "string or null",
  "franchises": ["array of strings – team name + league for sports teams, single entry for other IPs, or empty array"],
  "character": "string or null",
  "product_type": "string (mandatory)",
  "category": "string (mandatory – one of: Collectable, Toy, Sports, Clothing, Accessory)",
  "age": "string or null (e.g. 'Age 14+', always prefix with 'Age ')",
  "gender": "string or null (Male, Female, Unisex)",
  "price_band": "string (e.g. 'Price 20', always prefix with 'Price ', round £ to nearest whole number)",
  "size": "string or null",
  "sport_type": "string or null",
  "optional_tags": ["list of strings or empty list"],
  "escalation": ["list of field names that need manual review, or empty list"],
  "escalation_notes": "string explaining why fields were escalated, or empty string",
  "confidence": "high, medium, or low"
}

## RULES
- Only include tags that are factually supported or validated through controlled inference.
- No filler or speculative tagging.
- If you cannot confidently determine a field, set it to null and add the field name to "escalation".
- Do NOT guess. Flag for review instead.
"""


# ---------------------------------------------------------------------------
# SHOPIFY API CLIENT
# ---------------------------------------------------------------------------

class ShopifyClient:
    """Handles all Shopify Admin API interactions."""

    def __init__(self, store_url: str, access_token: str):
        self.base_url = f"https://{store_url}/admin/api/{SHOPIFY_API_VERSION}"
        self.headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json",
        }

    def _request(self, method: str, endpoint: str, payload: dict = None) -> dict:
        """Make a rate-limited request to Shopify."""
        url = f"{self.base_url}/{endpoint}"
        time.sleep(SHOPIFY_RATE_LIMIT_DELAY)

        try:
            if method == "GET":
                resp = requests.get(url, headers=self.headers, timeout=30)
            elif method == "PUT":
                resp = requests.put(url, headers=self.headers, json=payload, timeout=30)
            else:
                raise ValueError(f"Unsupported method: {method}")

            # Handle rate limiting
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", 2))
                log.warning(f"Rate limited. Waiting {retry_after}s...")
                time.sleep(retry_after)
                return self._request(method, endpoint, payload)

            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.RequestException as e:
            log.error(f"Shopify API error: {e}")
            raise

    def get_products_count(self) -> int:
        """Get total active product count."""
        data = self._request("GET", "products/count.json?status=active")
        return data.get("count", 0)

    def get_products(self, limit: int = 250, page_info: str = None) -> tuple:
        """
        Fetch products with cursor-based pagination.
        Returns (products_list, next_page_info).
        """
        endpoint = f"products.json?limit={limit}&status=active&fields=id,title,body_html,product_type,tags,variants,vendor,status"
        if page_info:
            endpoint = f"products.json?limit={limit}&page_info={page_info}"

        # For cursor pagination, we need to handle Link headers
        url = f"{self.base_url}/{endpoint}"
        time.sleep(SHOPIFY_RATE_LIMIT_DELAY)

        resp = requests.get(url, headers=self.headers, timeout=30)

        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", 2))
            log.warning(f"Rate limited. Waiting {retry_after}s...")
            time.sleep(retry_after)
            return self.get_products(limit, page_info)

        resp.raise_for_status()

        # Extract next page cursor from Link header
        next_page = None
        link_header = resp.headers.get("Link", "")
        if 'rel="next"' in link_header:
            for part in link_header.split(","):
                if 'rel="next"' in part:
                    url_part = part.split(";")[0].strip().strip("<>")
                    if "page_info=" in url_part:
                        next_page = url_part.split("page_info=")[1].split("&")[0]

        products = resp.json().get("products", [])
        return products, next_page

    def get_all_products(self) -> list:
        """Fetch ALL products using cursor pagination."""
        all_products = []
        page_info = None
        page_num = 0

        while True:
            page_num += 1
            products, next_page = self.get_products(limit=250, page_info=page_info)
            all_products.extend(products)
            log.info(f"  Fetched page {page_num}: {len(products)} products (total: {len(all_products)})")

            if not next_page or not products:
                break
            page_info = next_page
        return all_products

    def get_product(self, product_id: int) -> dict:
        """Fetch a single product by ID."""
        data = self._request("GET", f"products/{product_id}.json")
        return data.get("product", {})

    def update_product_tags(self, product_id: int, tags: str) -> dict:
        """Update a product's tags."""
        payload = {"product": {"id": product_id, "tags": tags}}
        return self._request("PUT", f"products/{product_id}.json", payload)


# ---------------------------------------------------------------------------
# CLAUDE AI TAGGER
# ---------------------------------------------------------------------------

class ClaudeTagger:
    """Uses Claude API to intelligently tag products."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.api_url = "https://api.anthropic.com/v1/messages"
        self.headers = {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }

    def tag_product(self, product: dict) -> dict:
        """Send product data to Claude and get structured tags back."""
        # Extract price from first variant
        price = "0.00"
        if product.get("variants"):
            price = product["variants"][0].get("price", "0.00")

        # Clean HTML from description
        description = product.get("body_html", "") or ""
        description = self._strip_html(description)

        # Truncate very long descriptions to save tokens
        if len(description) > 2000:
            description = description[:2000] + "..."

        user_message = f"""Tag this product according to the framework.

PRODUCT TITLE: {product.get('title', 'Unknown')}
PRODUCT DESCRIPTION: {description}
PRODUCT TYPE (from Shopify): {product.get('product_type', '')}
VENDOR: {product.get('vendor', '')}
PRICE: £{price}
EXISTING TAGS: {product.get('tags', '')}

Return ONLY the JSON object. No other text."""

        time.sleep(CLAUDE_RATE_LIMIT_DELAY)

        payload = {
            "model": CLAUDE_MODEL,
            "max_tokens": 1024,
            "system": TAGGING_SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": user_message}],
        }

        try:
            resp = requests.post(
                self.api_url,
                headers=self.headers,
                json=payload,
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()

            # Extract text response
            text = data["content"][0]["text"].strip()

            # Parse JSON from response (handle markdown code blocks)
            if text.startswith("```"):
                text = text.split("\n", 1)[1]  # Remove opening ```json
                text = text.rsplit("```", 1)[0]  # Remove closing ```

            tag_data = json.loads(text)
            return tag_data

        except json.JSONDecodeError as e:
            log.error(f"Failed to parse Claude response for product {product.get('id')}: {e}")
            log.error(f"Raw response: {text[:500]}")
            return {"error": str(e), "escalation": ["all"], "escalation_notes": "Failed to parse AI response"}

        except requests.exceptions.RequestException as e:
            log.error(f"Claude API error for product {product.get('id')}: {e}")
            return {"error": str(e), "escalation": ["all"], "escalation_notes": "API call failed"}

    @staticmethod
    def _strip_html(html: str) -> str:
        """Basic HTML tag removal."""
        import re
        clean = re.sub(r"<[^>]+>", " ", html)
        clean = re.sub(r"\s+", " ", clean).strip()
        return clean


# ---------------------------------------------------------------------------
# TAG BUILDER
# ---------------------------------------------------------------------------

def build_tag_string(tag_data: dict, existing_tags: str = "") -> str:
    """
    Convert Claude's structured tag JSON into a Shopify-compatible
    comma-separated tag string. Merges with existing tags intelligently.
    """
    new_tags = []

    # A) Brand
    if tag_data.get("brand"):
        new_tags.append(tag_data["brand"])

    # B) Franchise / League / IP (supports array for team + league)
    for franchise in tag_data.get("franchises", []):
        if franchise:
            new_tags.append(franchise)
    # Also support legacy single "franchise" field
    if tag_data.get("franchise") and not tag_data.get("franchises"):
        new_tags.append(tag_data["franchise"])

    # C) Character / Player
    if tag_data.get("character"):
        new_tags.append(tag_data["character"])

    # D) Product Type
    if tag_data.get("product_type"):
        new_tags.append(tag_data["product_type"])

    # E) Category
    if tag_data.get("category"):
        new_tags.append(tag_data["category"])

    # F) Age – always formatted as "Age X+"
    if tag_data.get("age"):
        age_val = str(tag_data["age"]).strip()
        if not age_val.lower().startswith("age "):
            age_val = f"Age {age_val}"
        new_tags.append(age_val)

    # G) Gender
    if tag_data.get("gender"):
        new_tags.append(tag_data["gender"])

    # H) Price Band – always formatted as "Price X"
    if tag_data.get("price_band") is not None:
        price_val = str(tag_data["price_band"]).strip()
        # Strip "Price " prefix if AI already added it, then re-add consistently
        price_val = price_val.replace("Price ", "").replace("price ", "").strip()
        new_tags.append(f"Price {price_val}")

    # I) Size
    if tag_data.get("size"):
        new_tags.append(tag_data["size"])

    # J) Sport Type
    if tag_data.get("sport_type"):
        new_tags.append(tag_data["sport_type"])

    # Optional tags
    for opt_tag in tag_data.get("optional_tags", []):
        if opt_tag:
            new_tags.append(opt_tag)

    # Deduplicate while preserving order
    seen = set()
    unique_tags = []
    for tag in new_tags:
        tag_lower = tag.strip().lower()
        if tag_lower not in seen:
            seen.add(tag_lower)
            unique_tags.append(tag.strip())

    return ", ".join(unique_tags)


# ---------------------------------------------------------------------------
# ESCALATION REPORT
# ---------------------------------------------------------------------------

class EscalationReport:
    """Collects and exports products that need manual review."""

    def __init__(self):
        self.items = []

    def add(self, product: dict, tag_data: dict):
        """Add a product to the escalation report."""
        self.items.append({
            "product_id": product.get("id"),
            "product_title": product.get("title", ""),
            "unclear_fields": ", ".join(tag_data.get("escalation", [])),
            "escalation_notes": tag_data.get("escalation_notes", ""),
            "confidence": tag_data.get("confidence", "unknown"),
            "suggested_tags": build_tag_string(tag_data),
            "shopify_url": f"https://{SHOPIFY_STORE_URL}/admin/products/{product.get('id')}",
        })

    def export(self, filepath: Path):
        """Export escalation report as CSV."""
        if not self.items:
            log.info("No escalations – all products tagged confidently.")
            return

        fieldnames = [
            "product_id", "product_title", "unclear_fields",
            "escalation_notes", "confidence", "suggested_tags", "shopify_url",
        ]
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.items)

        log.info(f"Escalation report: {len(self.items)} products → {filepath}")


# ---------------------------------------------------------------------------
# MAIN TAGGER ENGINE
# ---------------------------------------------------------------------------

class InfinityTagger:
    """Orchestrates the full tagging workflow."""

    def __init__(self, dry_run: bool = False):
        self.shopify = ShopifyClient(SHOPIFY_STORE_URL, SHOPIFY_ACCESS_TOKEN)
        self.claude = ClaudeTagger(ANTHROPIC_API_KEY)
        self.escalation = EscalationReport()
        self.dry_run = dry_run
        self.tag_counter = {}   # tracks all unique tags and how often applied
        self.stats = {
            "total": 0,
            "tagged": 0,
            "escalated": 0,
            "skipped": 0,
            "errors": 0,
        }

    def run(self, new_only: bool = False, product_id: int = None, batch_size: int = DEFAULT_BATCH_SIZE):
        """Execute the tagging run."""
        log.info("=" * 60)
        log.info("INFINITY COLLECTABLES – AUTO TAGGER v1.0")
        log.info(f"Mode: {'DRY RUN' if self.dry_run else 'LIVE'}")
        log.info(f"Store: {SHOPIFY_STORE_URL}")
        log.info(f"Time: {datetime.now(timezone.utc).isoformat()}")
        log.info("=" * 60)

        # Fetch products
        if product_id:
            log.info(f"Fetching single product: {product_id}")
            product = self.shopify.get_product(product_id)
            products = [product] if product else []
        else:
            total_count = self.shopify.get_products_count()
            log.info(f"Total products in store: {total_count}")
            log.info("Fetching all products...")
            products = self.shopify.get_all_products()

        if not products:
            log.warning("No products found.")
            return

        # Filter to new/untagged only if requested
        if new_only:
            products = [p for p in products if not p.get("tags", "").strip()]
            log.info(f"Filtered to {len(products)} untagged products")

        self.stats["total"] = len(products)
        log.info(f"Processing {len(products)} products...")

        # Process in batches
        total_batches = math.ceil(len(products) / batch_size)
        for batch_num in range(total_batches):
            start = batch_num * batch_size
            end = start + batch_size
            batch = products[start:end]

            log.info(f"\n--- Batch {batch_num + 1}/{total_batches} ({len(batch)} products) ---")

            for product in batch:
                self._process_product(product)

        # Export reports
        self._export_reports()

        # Print summary
        self._print_summary()

    def _process_product(self, product: dict):
        """Process a single product through the tagging pipeline."""
        pid = product.get("id")
        title = product.get("title", "Unknown")

        log.info(f"  [{pid}] {title}")

        try:
            # Get tags from Claude
            tag_data = self.claude.tag_product(product)

            if tag_data.get("error"):
                log.error(f"    ERROR: {tag_data['error']}")
                self.escalation.add(product, tag_data)
                self.stats["errors"] += 1
                return

            # Check for escalations
            escalations = tag_data.get("escalation", [])
            confidence = tag_data.get("confidence", "unknown")

            if escalations:
                log.warning(f"    ESCALATE: {escalations} (confidence: {confidence})")
                self.escalation.add(product, tag_data)
                self.stats["escalated"] += 1

                # Still apply the tags we're confident about
                if confidence == "low":
                    log.warning(f"    SKIPPED (low confidence)")
                    self.stats["skipped"] += 1
                    return

            # Build tag string
            new_tags = build_tag_string(tag_data, product.get("tags", ""))

            log.info(f"    TAGS: {new_tags}")

            # Apply tags (unless dry run)
            if not self.dry_run:
                self.shopify.update_product_tags(pid, new_tags)
                log.info(f"    ✓ Applied")
            else:
                log.info(f"    [DRY RUN – not applied]")

            # Track every individual tag for the summary report
            for t in [tag.strip() for tag in new_tags.split(",") if tag.strip()]:
                self.tag_counter[t] = self.tag_counter.get(t, 0) + 1

            self.stats["tagged"] += 1

        except Exception as e:
            log.error(f"    FAILED: {e}")
            self.stats["errors"] += 1

    def _export_reports(self):
        """Export all reports."""
        # Escalation CSV
        self.escalation.export(OUTPUT_DIR / f"escalation_report_{TODAY}.csv")

        # Full run log as JSON
        summary = {
            "run_date": TODAY,
            "run_time": datetime.now(timezone.utc).isoformat(),
            "mode": "dry_run" if self.dry_run else "live",
            "store": SHOPIFY_STORE_URL,
            "stats": self.stats,
        }
        with open(OUTPUT_DIR / f"tagger_summary_{TODAY}.json", "w") as f:
            json.dump(summary, f, indent=2)

        # Tag summary CSV – sorted by frequency, then alphabetically
        tag_summary_path = OUTPUT_DIR / f"tag_summary_{TODAY}.csv"
        sorted_tags = sorted(self.tag_counter.items(), key=lambda x: (-x[1], x[0]))
        with open(tag_summary_path, "w", newline="", encoding="utf-8") as f:
            writer = __import__("csv").writer(f)
            writer.writerow(["Tag", "Products Tagged"])
            writer.writerows(sorted_tags)
        log.info(f"Tag summary: {len(sorted_tags)} unique tags → {tag_summary_path.name}")

    def _print_summary(self):
        """Print run summary."""
        log.info("\n" + "=" * 60)
        log.info("RUN COMPLETE")
        log.info("=" * 60)
        log.info(f"  Total products processed: {self.stats['total']}")
        log.info(f"  Successfully tagged:      {self.stats['tagged']}")
        log.info(f"  Escalated for review:     {self.stats['escalated']}")
        log.info(f"  Skipped (low confidence): {self.stats['skipped']}")
        log.info(f"  Errors:                   {self.stats['errors']}")
        log.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def validate_config():
    """Validate required environment variables."""
    errors = []
    if not SHOPIFY_STORE_URL:
        errors.append("SHOPIFY_STORE_URL not set in .env")
    if not SHOPIFY_ACCESS_TOKEN:
        errors.append("SHOPIFY_ACCESS_TOKEN not set in .env")
    if not ANTHROPIC_API_KEY:
        errors.append("ANTHROPIC_API_KEY not set in .env")
    if errors:
        for e in errors:
            log.error(e)
        log.error("See SETUP_GUIDE.md for configuration instructions.")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Infinity Collectables – Automated Product Tagger"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview tags without writing to Shopify",
    )
    parser.add_argument(
        "--new-only",
        action="store_true",
        help="Only tag products with no existing tags",
    )
    parser.add_argument(
        "--product-id",
        type=int,
        help="Tag a specific product by Shopify ID",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Products per batch (default: {DEFAULT_BATCH_SIZE})",
    )

    args = parser.parse_args()

    validate_config()

    tagger = InfinityTagger(dry_run=args.dry_run)
    tagger.run(
        new_only=args.new_only,
        product_id=args.product_id,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()
