"""
Infinity Collectables – Shopify Ops Dashboard
=============================================
Password:  set APP_PASSWORD in .env (default: infinity2024)
Deploy:    streamlit run app.py
Secrets:   SHOPIFY_STORE_URL, SHOPIFY_ACCESS_TOKEN, ANTHROPIC_API_KEY, APP_PASSWORD
"""

import streamlit as st
import requests
import os
import sys
import json
import subprocess
import time
import hashlib
import re
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
SHOPIFY_STORE   = os.getenv("SHOPIFY_STORE_URL", "").strip().strip("/")
ACCESS_TOKEN    = os.getenv("SHOPIFY_ACCESS_TOKEN", "")
ANTHROPIC_KEY   = os.getenv("ANTHROPIC_API_KEY", "")
API_VERSION     = "2024-01"
BASE_URL        = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}"
STORE_DOMAIN    = f"https://{SHOPIFY_STORE.replace('.myshopify.com', '')}.com" \
                  if SHOPIFY_STORE else "https://infinitycollectables.co.uk"
HEADERS         = {"X-Shopify-Access-Token": ACCESS_TOKEN,
                   "Content-Type": "application/json"}
TAGGER_SCRIPT   = os.path.join(os.path.dirname(__file__), "infinity_tagger.py")

def _tag_csv_path():
    out = os.path.join(os.path.dirname(__file__), "outputs")
    if os.path.isdir(out):
        hits = sorted([f for f in os.listdir(out)
                       if f.startswith("tag_summary") and f.endswith(".csv")], reverse=True)
        if hits:
            return os.path.join(out, hits[0])
    return None

TAG_CSV = _tag_csv_path()

TIER_COLOURS = {"Tier 1":"#1A7F3C","Tier 2":"#2E6DA4","Tier 3":"#E07B00","Tier 4":"#8E44AD"}

COLLECTIONS_PLAN = [
    ("Tier 1","All Football","Sport","Football",3982,"Broadest football landing page"),
    ("Tier 1","All Collectables","Category","Collectable",2363,"Homepage anchor for collector audience"),
    ("Tier 1","Age 14+","Age","Age 14+",1655,"Adult collectors; NECA, Mezco premium lines"),
    ("Tier 1","Premier League","League","Premier League",3081,"Biggest PL keyword cluster"),
    ("Tier 1","Action Figures","Product Type","Action Figure",647,"Highest product-type volume"),
    ("Tier 1","Harry Potter","Franchise","Harry Potter",596,"#1 franchise by volume"),
    ("Tier 1","Clothing","Product Type","Clothing",555,"All wearables"),
    ("Tier 1","NECA","Brand","NECA",485,"High AOV brand collectors"),
    ("Tier 1","Age 6+","Age","Age 6+",406,"Kids + gifting segment"),
    ("Tier 1","Age 3+","Age","Age 3+",405,"Toddler/younger kids"),
    ("Tier 1","Keychains","Product Type","Keychain",341,"High-volume impulse buy"),
    ("Tier 1","Scottish Premiership","League","Scottish Premiership",339,"Dedicated Scottish fan base"),
    ("Tier 1","T-Shirts","Product Type","T-Shirt",323,"Targeted SEO opportunity"),
    ("Tier 1","Vinyl Figures","Product Type","Vinyl Figure",308,"Funko-adjacent audience"),
    ("Tier 1","Rings","Product Type","Ring",293,"Jewellery gifting segment"),
    ("Tier 1","TMNT","Franchise","TMNT",288,"Nostalgia + new film audience"),
    ("Tier 1","Liverpool FC","Football Club","Liverpool FC",516,"Largest club collection"),
    ("Tier 1","Arsenal FC","Football Club","Arsenal FC",389,"2nd largest club"),
    ("Tier 1","Chelsea FC","Football Club","Chelsea FC",375,"3rd largest club"),
    ("Tier 1","Tottenham Hotspur","Football Club","Tottenham",283,"Strong London fan demand"),
    ("Tier 2","West Ham United","Football Club","West Ham",272,"272 products"),
    ("Tier 2","Manchester City","Football Club","Man City",252,"Champions League audience"),
    ("Tier 2","Mugs","Product Type","Mug",212,"Popular gift item"),
    ("Tier 2","Newcastle United","Football Club","Newcastle",231,"Rapidly growing fan base"),
    ("Tier 2","Celtic FC","Football Club","Celtic FC",212,"Scottish institution; diaspora market"),
    ("Tier 2","Bracelets","Product Type","Bracelet",187,"Gift/jewellery crossover"),
    ("Tier 2","Posters","Product Type","Poster",187,"Room decor segment"),
    ("Tier 2","Disney","Franchise","Disney",180,"Broad character range"),
    ("Tier 2","La Liga","League","La Liga",181,"Spanish football fans"),
    ("Tier 2","Marvel","Franchise","Marvel",169,"Evergreen cinematic universe"),
    ("Tier 2","Age 8+","Age","Age 8+",150,"Mid-range kids gifting"),
    ("Tier 2","SoccerStarz","Brand","SoccerStarz",178,"Strong repeat buyers"),
    ("Tier 3","Rangers FC","Football Club","Rangers FC",136,"Strong Scottish demand"),
    ("Tier 3","DC Comics","Franchise","DC Comics",131,"Batman/Superman audience"),
    ("Tier 3","Pins","Product Type","Pin",143,"Low-cost impulse"),
    ("Tier 3","Cufflinks","Product Type","Cufflinks",130,"Corporate gift niche"),
    ("Tier 3","Star Wars","Franchise","Star Wars",118,"Perennial IP"),
    ("Tier 3","Manchester United","Football Club","Man United",110,"Global brand"),
    ("Tier 3","Plush","Product Type","Plush",128,"Soft toy / kids gift"),
    ("Tier 3","Caps","Product Type","Cap",109,"Football cap gift crossover"),
    ("Tier 3","Wallets","Product Type","Wallet",107,"Men's gift segment"),
    ("Tier 3","MINIX","Brand","MINIX",98,"Dedicated niche audience"),
    ("Tier 3","Nintendo","Franchise","Nintendo",98,"Gaming collectables"),
    ("Tier 3","Trading Cards","Product Type","Trading Cards",99,"Growing collector market"),
    ("Tier 3","Trick or Treat Studios","Brand","Trick or Treat Studios",93,"Horror niche"),
    ("Tier 3","Masks","Product Type","Mask",92,"Horror/cosplay niche"),
    ("Tier 3","Aston Villa","Football Club","Aston Villa",94,"UCL returnees"),
    ("Tier 3","Everton","Football Club","Everton",91,"New stadium era"),
    ("Tier 3","Sanrio","Franchise","Sanrio",87,"Hello Kitty; TikTok-driven demand"),
    ("Tier 3","FC Barcelona","Football Club","FC Barcelona",82,"International Spanish fans"),
    ("Tier 3","Batman","Franchise","Batman",79,"Evergreen DC icon"),
    ("Tier 3","Bushiroad","Brand","Bushiroad",79,"TCG / anime cards"),
    ("Tier 3","Towels","Product Type","Towel",79,"Football gift staple"),
    ("Tier 3","Stranger Things","Franchise","Stranger Things",78,"Netflix nostalgia"),
    ("Tier 3","Lilo & Stitch","Franchise","Lilo & Stitch",76,"Trending Disney character"),
    ("Tier 3","Super Mario","Franchise","Super Mario",76,"Gaming collectors"),
    ("Tier 3","Leicester City","Football Club","Leicester FC",76,"Championship returnees"),
    ("Tier 4","Mezco Toyz","Brand","Mezco Toyz",71,"Premium collector figures"),
    ("Tier 4","Beanies","Product Type","Beanie",75,"Winter gifting"),
    ("Tier 4","Duvet Sets","Product Type","Duvet Set",75,"Bedroom décor gifting"),
    ("Tier 4","Drinks Bottles","Product Type","Drinks Bottle",74,"Functional gift"),
    ("Tier 4","Limited Edition","Special","Limited Edition",74,"Scarcity marketing"),
    ("Tier 4","Puzzles","Product Type","Puzzle",70,"Family gifting"),
    ("Tier 4","Necklaces","Product Type","Necklace",70,"Jewellery gifting"),
    ("Tier 4","Wednesday","Franchise","Wednesday",59,"Netflix - Addams Family fandom"),
    ("Tier 4","Stickers","Product Type","Sticker",66,"Low-AOV add-on"),
    ("Tier 4","Backpacks","Product Type","Backpack",66,"School/fan crossover"),
    ("Tier 4","Scarves","Product Type","Scarf",62,"Football match-day staple"),
    ("Tier 4","Real Madrid","Football Club","Real Madrid",62,"Champions League audience"),
    ("Tier 4","Wall Signs","Product Type","Wall Sign",60,"Man-cave/fan-room niche"),
    ("Tier 4","Crystal Palace","Football Club","Crystal Palace",50,"South London fan base"),
    ("Tier 4","Age 13+","Age","Age 13+",58,"Teen collector segment"),
    ("Tier 4","Notebooks","Product Type","Notebook",56,"Stationery gifts"),
    ("Tier 4","Gifts for Him","Gender","Male",359,"Gift guide SEO"),
    ("Tier 4","Gifts for Her","Gender","Female",324,"Gift guide SEO"),
]


# ══════════════════════════════════════════════════════════════════════════════
# AUTH
# ══════════════════════════════════════════════════════════════════════════════
def _check_password() -> bool:
    correct_pw   = os.getenv("APP_PASSWORD", "infinity2024")
    correct_hash = hashlib.sha256(correct_pw.encode()).hexdigest()
    if st.session_state.get("authenticated"):
        return True
    _, col, _ = st.columns([1, 1.6, 1])
    with col:
        st.markdown(
            "<div style='text-align:center;margin-top:80px'>"
            "<h2 style='color:#0D1B2A'>♾️ Infinity Collectables</h2>"
            "<p style='color:#888'>Ops Dashboard - Team Access</p></div>",
            unsafe_allow_html=True)
        with st.form("login_form"):
            pw = st.text_input("Password", type="password",
                               placeholder="Enter team password")
            ok = st.form_submit_button("Log In", use_container_width=True,
                                       type="primary")
        if ok:
            if hashlib.sha256(pw.encode()).hexdigest() == correct_hash:
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("Incorrect password.")
    return False


# ══════════════════════════════════════════════════════════════════════════════
# SHOPIFY API HELPERS
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_data(ttl=120)
def fetch_existing_collections():
    existing = {}   # title_lower → handle
    if not ACCESS_TOKEN or not SHOPIFY_STORE:
        return existing
    for endpoint in ("smart_collections", "custom_collections"):
        url = f"{BASE_URL}/{endpoint}.json?limit=250&fields=id,title,handle"
        while url:
            r = requests.get(url, headers=HEADERS)
            if r.status_code != 200:
                break
            for c in r.json().get(endpoint, []):
                existing[c["title"].lower()] = c["handle"]
            link = r.headers.get("Link", "")
            url  = next((p.split("<")[1].split(">")[0]
                         for p in link.split(",") if 'rel="next"' in p), None)
    return existing


@st.cache_data(ttl=300)
def fetch_all_live_collections():
    """Return list of (title, handle) for all live collections - for blog interlinking."""
    results = []
    if not ACCESS_TOKEN or not SHOPIFY_STORE:
        return results
    for endpoint in ("smart_collections", "custom_collections"):
        url = f"{BASE_URL}/{endpoint}.json?limit=250&fields=title,handle"
        while url:
            r = requests.get(url, headers=HEADERS)
            if r.status_code != 200:
                break
            for c in r.json().get(endpoint, []):
                results.append((c["title"], c["handle"]))
            link = r.headers.get("Link", "")
            url  = next((p.split("<")[1].split(">")[0]
                         for p in link.split(",") if 'rel="next"' in p), None)
    return sorted(results, key=lambda x: x[0])


@st.cache_data(ttl=120)
def search_shopify_products(query: str, limit: int = 12):
    """Search products by title/tag for blog featuring."""
    if not ACCESS_TOKEN or not SHOPIFY_STORE or not query:
        return []
    url = (f"{BASE_URL}/products.json?limit={limit}&status=active"
           f"&fields=id,title,handle,variants,images"
           f"&title={requests.utils.quote(query)}")
    r = requests.get(url, headers=HEADERS)
    if r.status_code != 200:
        return []
    return r.json().get("products", [])


@st.cache_data(ttl=300)
def fetch_shopify_blogs():
    """Return list of (id, title) for all blogs."""
    if not ACCESS_TOKEN or not SHOPIFY_STORE:
        return []
    r = requests.get(f"{BASE_URL}/blogs.json", headers=HEADERS)
    if r.status_code != 200:
        return []
    return [(b["id"], b["title"]) for b in r.json().get("blogs", [])]


def create_smart_collection(title: str, tag: str, sort_by="best-selling",
                             body_html="", seo_title="", seo_description="") -> dict:
    sc = {
        "title": title,
        "rules": [{"column": "tag", "relation": "equals", "condition": tag}],
        "disjunctive": False,
        "sort_order": sort_by,
    }
    if body_html:
        sc["body_html"] = body_html
    if seo_title or seo_description:
        sc["metafields"] = []
        if seo_title:
            sc["metafields"].append({
                "key": "title_tag", "value": seo_title,
                "type": "single_line_text_field", "namespace": "global",
            })
        if seo_description:
            sc["metafields"].append({
                "key": "description_tag", "value": seo_description,
                "type": "single_line_text_field", "namespace": "global",
            })
    return requests.post(f"{BASE_URL}/smart_collections.json",
                         headers=HEADERS, json={"smart_collection": sc}).json()


def publish_article_to_shopify(blog_id: int, title: str, body_html: str,
                                summary: str, tags: str) -> dict:
    payload = {"article": {
        "title": title,
        "body_html": body_html,
        "summary_html": summary,
        "tags": tags,
        "published": True,
    }}
    return requests.post(f"{BASE_URL}/blogs/{blog_id}/articles.json",
                         headers=HEADERS, json=payload).json()


def parse_natural_language(text: str):
    text_lower = text.lower()
    for _, name, _, tag, _, _ in COLLECTIONS_PLAN:
        if name.lower() in text_lower or tag.lower() in text_lower:
            return name, tag
    return None, None


# ══════════════════════════════════════════════════════════════════════════════
# BLOG GENERATION
# ══════════════════════════════════════════════════════════════════════════════
def build_blog_prompt(topic: str, post_type: str, keyword: str, word_count: int,
                      collections: list, products: list) -> str:
    """Build the Claude prompt following the skill's SEO+GEO structure."""
    store_url = STORE_DOMAIN

    # Format live collections as markdown links
    coll_block = ""
    if collections:
        coll_block = "\n".join(
            f"- [{t}]({store_url}/collections/{h})" for t, h in collections[:20]
        )

    # Format featured products
    prod_block = ""
    if products:
        lines = []
        for p in products[:8]:
            price = ""
            if p.get("variants"):
                price = f" - £{p['variants'][0].get('price','')}"
            lines.append(f"- [{p['title']}]({store_url}/products/{p['handle']}){price}")
        prod_block = "\n".join(lines)

    return f"""You are a content writer for Infinity Collectables, a UK-based online retailer selling pop culture toys, games, and collectibles. Write a high-quality blog post following ALL the requirements below exactly.

---
BRIEF
- Topic: {topic}
- Post type: {post_type}
- Primary keyword: {keyword}
- Target word count: ~{word_count} words
- Audience: UK fans, collectors, and gift buyers aged 16–45
- Tone: Knowledgeable, enthusiastic, trustworthy - collector-to-collector energy

---
LIVE COLLECTIONS TO LINK (use the most relevant ones as natural internal links):
{coll_block if coll_block else "Use /collections/[handle] URLs as appropriate"}

---
FEATURED PRODUCTS TO MENTION (weave in naturally with actual links):
{prod_block if prod_block else "Reference products naturally without specific links"}

---
REQUIRED STRUCTURE - follow this exactly:

## [SEO Title - H1, 55–65 chars, include primary keyword]

**Quick Answer / TL;DR**
- [3–6 bullet points summarising the post]

### Introduction (80–120 words)
Hook in first sentence. State what the post covers. Include primary keyword naturally.

[Place first CTA block here]:
> 🛒 **Browse our [most relevant collection name]** → [link to collection]

### [H2 section 1]
2–4 sentence paragraphs. Add a natural internal link to a collection every 300 words.

### [H2 section 2]

### [H2 section 3]
...continue as needed for the post type...

[Comparison table if relevant]

### Common Mistakes / What to Avoid

### FAQ
**Q: [question 1]**
[2–3 sentence answer]

**Q: [question 2]**
[answer]

**Q: [question 3]**
[answer]

**Q: [question 4]**
[answer]

### Conclusion (1 short paragraph + final CTA)
> 🛒 **Shop [collection name]** → [link]

---
SEO METADATA (provide after the post):

**Primary keyword:** [keyword]
**Secondary keywords:** [5–8 comma-separated]
**Meta title (55–60 chars):** [title]
**Meta description (150–155 chars):** [description]
**Recommended schema:** Article + FAQPage
**Hero image alt text:** [one line]

---
QUALITY RULES:
- TL;DR box must be present and scannable
- Primary keyword in H1, first paragraph, at least 2 H2s, and meta description
- At least 3 specific citable facts (brand names, prices, scales, materials)
- FAQ with minimum 4 real questions
- At least 3 internal links to live collection pages using the URLs provided above
- Two CTA blocks (one near top, one at bottom)
- No keyword stuffing - must read naturally aloud
- Do not use em dashes - use commas or rewrite the sentence instead
- Write in British English (licence not license, colour not color etc.)

Now write the complete blog post:"""


def call_anthropic(prompt: str) -> str:
    """Call Claude Haiku via Anthropic Messages API and return the text."""
    if not ANTHROPIC_KEY:
        return "ERROR: ANTHROPIC_API_KEY not set in .env / secrets."
    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 4096,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=120,
    )
    if r.status_code != 200:
        return f"API error {r.status_code}: {r.text}"
    return r.json()["content"][0]["text"]


def generate_collection_content(name: str, tag: str,
                                  ctype: str = "", note: str = "") -> dict:
    """Use Claude Haiku to generate body_html, seo_title, and seo_description
    for a Shopify collection. Returns empty strings if no API key."""
    empty = {"body_html": "", "seo_title": "", "seo_description": ""}
    if not ANTHROPIC_KEY:
        return empty
    prompt = (
        "You are writing content for Infinity Collectables (infinitycollectables.co.uk), "
        "a UK online retailer specialising in collectables, figures, toys, and memorabilia.\n\n"
        f"Write content for a Shopify smart collection:\n"
        f"- Name: {name}\n"
        f"- Tag filter: {tag}\n"
        f"- Type: {ctype or 'General'}\n"
        f"- Notes: {note or 'None'}\n\n"
        "Return ONLY a JSON object, no markdown fences, no extra text:\n"
        "{\n"
        '  "body_html": "<p>[2-3 natural sentences describing this collection. '
        "Mention specific brands, product types, or themes. "
        "End with a soft call-to-action. British English, no em dashes.]</p>\",\n"
        '  "seo_title": "[Collection Name] | Infinity Collectables",\n'
        '  "seo_description": "[150-155 char meta description. '
        "Lead with primary keyword. Mention UK delivery. Ends with CTA. British English.]\"\n"
        "}"
    )
    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 512,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=30,
    )
    if r.status_code != 200:
        return empty
    text = r.json()["content"][0]["text"].strip()
    # Strip markdown fences if present
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()
    try:
        return json.loads(text)
    except Exception:
        return empty


def markdown_to_html(md: str) -> str:
    """Very lightweight markdown → HTML for Shopify blog body."""
    html = md
    # H1–H3
    html = re.sub(r"^### (.+)$", r"<h3>\1</h3>", html, flags=re.MULTILINE)
    html = re.sub(r"^## (.+)$",  r"<h2>\1</h2>", html, flags=re.MULTILINE)
    html = re.sub(r"^# (.+)$",   r"<h1>\1</h1>", html, flags=re.MULTILINE)
    # Bold
    html = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", html)
    # Links
    html = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', html)
    # Blockquotes (CTAs)
    html = re.sub(r"^> (.+)$", r"<p><em>\1</em></p>", html, flags=re.MULTILINE)
    # Bullets
    html = re.sub(r"^- (.+)$", r"<li>\1</li>", html, flags=re.MULTILINE)
    html = re.sub(r"(<li>.*</li>)", r"<ul>\1</ul>", html, flags=re.DOTALL)
    # Paragraphs
    lines = html.split("\n")
    out   = []
    for ln in lines:
        s = ln.strip()
        if s and not s.startswith("<"):
            s = f"<p>{s}</p>"
        out.append(s)
    return "\n".join(out)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: COLLECTIONS DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════
def page_dashboard():
    st.header("Collections Dashboard")
    st.caption("75 recommended collections from your product tags. "
               "Green = live in Shopify. Tap Create to build any missing ones.")

    if not ACCESS_TOKEN or not SHOPIFY_STORE:
        st.warning("Shopify credentials not found. Add SHOPIFY_STORE_URL and "
                   "SHOPIFY_ACCESS_TOKEN to your .env file.")
        return

    existing = fetch_existing_collections()

    # Filters - stack to 2 columns for mobile friendliness
    c1, c2 = st.columns(2)
    tier_filter   = c1.selectbox("Tier",   ["All","Tier 1","Tier 2","Tier 3","Tier 4"])
    status_filter = c2.selectbox("Status", ["All","To Build","Already Live"])
    type_filter   = st.selectbox("Type", ["All"] + sorted(set(c[2] for c in COLLECTIONS_PLAN)))

    filtered = [c for c in COLLECTIONS_PLAN
                if (tier_filter == "All" or c[0] == tier_filter)
                and (type_filter == "All" or c[2] == type_filter)]

    live    = [c for c in filtered if c[1].lower() in existing]
    tobuild = [c for c in filtered if c[1].lower() not in existing]

    if status_filter == "Already Live":
        filtered = live
    elif status_filter == "To Build":
        filtered = tobuild
    else:
        filtered = tobuild + live

    # Metrics
    m1, m2, m3 = st.columns(3)
    m1.metric("Total", 75)
    m2.metric("Live",  len([c for c in COLLECTIONS_PLAN if c[1].lower() in existing]))
    m3.metric("To Build", len([c for c in COLLECTIONS_PLAN if c[1].lower() not in existing]))

    st.divider()

    # Bulk create
    to_create = [c for c in filtered if c[1].lower() not in existing]
    if to_create and status_filter != "Already Live":
        if st.button(f"🚀 Create All {len(to_create)} Shown Collections",
                     type="primary", use_container_width=True):
            prog = st.progress(0)
            ok = 0
            for i, coll in enumerate(to_create):
                seo = generate_collection_content(coll[1], coll[3], coll[2], coll[5])
                res = create_smart_collection(coll[1], coll[3],
                                               body_html=seo["body_html"],
                                               seo_title=seo["seo_title"],
                                               seo_description=seo["seo_description"])
                if "smart_collection" in res:
                    ok += 1
                prog.progress((i + 1) / len(to_create))
                time.sleep(0.3)
            st.success(f"Done - {ok}/{len(to_create)} collections created.")
            fetch_existing_collections.clear()
            st.rerun()

    # List
    for tier, name, ctype, tag, est, note in filtered:
        is_live = name.lower() in existing
        colour  = TIER_COLOURS.get(tier, "#444")
        with st.container():
            st.markdown(
                f"<div style='display:flex;align-items:center;gap:8px;margin-bottom:2px'>"
                f"<span style='background:{colour};color:white;padding:2px 7px;"
                f"border-radius:4px;font-size:11px;white-space:nowrap'>{tier}</span>"
                f"<strong>{name}</strong>"
                f"<span style='color:#888;font-size:12px;margin-left:auto'>~{est:,} products</span>"
                f"</div>"
                f"<div style='color:#888;font-size:12px;margin-bottom:6px'>"
                f"{ctype} &nbsp;·&nbsp; tag: <code>{tag}</code> &nbsp;·&nbsp; {note}</div>",
                unsafe_allow_html=True)
            if is_live:
                st.success("✓ Live in Shopify", icon="✅")
            else:
                if st.button(f"Create → {name}", key=f"btn_{name}",
                             use_container_width=True):
                    with st.spinner(f"Creating '{name}'..."):
                        seo = generate_collection_content(name, tag, ctype, note)
                        res = create_smart_collection(name, tag,
                                                       body_html=seo["body_html"],
                                                       seo_title=seo["seo_title"],
                                                       seo_description=seo["seo_description"])
                    if "smart_collection" in res:
                        st.toast(f"'{name}' created!", icon="🎉")
                        fetch_existing_collections.clear()
                        st.rerun()
                    else:
                        st.error(str(res.get("errors", res)))
            st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: COLLECTION CREATOR
# ══════════════════════════════════════════════════════════════════════════════
def page_creator():
    st.header("Collection Creator")
    st.caption("Type a description or use the form to create a collection in Shopify.")

    if not ACCESS_TOKEN or not SHOPIFY_STORE:
        st.warning("Shopify credentials not found in .env file.")
        return

    tab1, tab2, tab3 = st.tabs(["Quick (Natural Language)", "Custom Form", "Bulk CSV"])

    with tab1:
        st.markdown("**Try:** *create a Man City collection*, *I need a Harry Potter page*, "
                    "*build a vinyl figures section*")
        user_input = st.text_input("Describe your collection",
                                    placeholder="e.g. create a Liverpool FC collection",
                                    label_visibility="collapsed")
        if st.button("Create Collection", type="primary",
                     key="nl_create", use_container_width=True):
            if not user_input.strip():
                st.warning("Describe the collection first.")
            else:
                title, tag = parse_natural_language(user_input)
                if title and tag:
                    with st.spinner(f"Creating '{title}'..."):
                        seo = generate_collection_content(title, tag)
                        res = create_smart_collection(title, tag,
                                                       body_html=seo["body_html"],
                                                       seo_title=seo["seo_title"],
                                                       seo_description=seo["seo_description"])
                    if "smart_collection" in res:
                        sc = res["smart_collection"]
                        st.success(f"✅ '{sc['title']}' created!")
                        st.code(f"Handle: {sc['handle']}\nTag filter: {tag}")
                        fetch_existing_collections.clear()
                    else:
                        st.error(str(res.get("errors", res)))
                else:
                    st.info("Couldn't match that to a known collection. "
                            "Use the Custom Form tab to set any tag.")

    with tab2:
        title      = st.text_input("Collection Name", placeholder="e.g. Harry Potter Gifts")
        tag        = st.text_input("Shopify Tag Filter", placeholder="e.g. Harry Potter")
        sort_order = st.selectbox("Sort by",
                                   ["best-selling","created-desc",
                                    "price-asc","price-desc","alpha-asc"])
        if st.button("Create Collection", type="primary",
                     key="manual_create", use_container_width=True):
            if not title or not tag:
                st.warning("Fill in both fields.")
            else:
                with st.spinner():
                    seo = generate_collection_content(title, tag)
                    res = create_smart_collection(title, tag, sort_order,
                                                   body_html=seo["body_html"],
                                                   seo_title=seo["seo_title"],
                                                   seo_description=seo["seo_description"])
                if "smart_collection" in res:
                    sc = res["smart_collection"]
                    st.success(f"✅ '{sc['title']}' created - handle: `{sc['handle']}`")
                    fetch_existing_collections.clear()
                else:
                    st.error(str(res.get("errors", res)))

    with tab3:
        st.caption("Upload a CSV with columns `name` and `tag`.")
        uploaded = st.file_uploader("Upload CSV", type=["csv"])
        if uploaded:
            df = pd.read_csv(uploaded)
            if "name" not in df.columns or "tag" not in df.columns:
                st.error("CSV needs 'name' and 'tag' columns.")
            else:
                st.dataframe(df, use_container_width=True)
                if st.button("Create All", type="primary", use_container_width=True):
                    prog = st.progress(0)
                    results = []
                    for i, row in df.iterrows():
                        seo = generate_collection_content(str(row["name"]), str(row["tag"]))
                        res = create_smart_collection(str(row["name"]), str(row["tag"]),
                                                       body_html=seo["body_html"],
                                                       seo_title=seo["seo_title"],
                                                       seo_description=seo["seo_description"])
                        results.append(
                            f"{'✅' if 'smart_collection' in res else '❌'} {row['name']}")
                        prog.progress((i + 1) / len(df))
                        time.sleep(0.3)
                    for r in results:
                        st.write(r)
                    fetch_existing_collections.clear()


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: BLOG WRITER
# ══════════════════════════════════════════════════════════════════════════════
def page_blog():
    st.header("Blog Writer")
    st.caption("Write SEO + GEO optimised blog posts with live collection and product links "
               "pulled straight from your Shopify store.")

    if not ANTHROPIC_KEY:
        st.warning("ANTHROPIC_API_KEY not set. Add it to your .env file or Streamlit secrets.")
        return

    # ── Brief ───────────────────────────────────────────────────────────────
    with st.expander("1. Brief", expanded=True):
        topic    = st.text_input("Blog topic or title idea",
                                  placeholder="e.g. Best Harry Potter gifts for adults")
        keyword  = st.text_input("Primary keyword",
                                  placeholder="e.g. Harry Potter gifts UK")
        post_type = st.selectbox("Post type", [
            "Buyer's guide / Best X for Y",
            "Gift guide",
            "How-to / Checklist",
            "Beginner's guide",
            "Comparison",
            "New release / Trend roundup",
            "Ultimate / Pillar guide",
        ])
        word_count = st.select_slider("Word count",
                                       options=[600, 800, 1000, 1200, 1500, 1800, 2000, 2500],
                                       value=1000)

    # ── Collections to interlink ─────────────────────────────────────────────
    with st.expander("2. Collections to Interlink", expanded=True):
        st.caption("Select live Shopify collections to embed as internal links in the post. "
                   "The AI picks the most relevant ones automatically - you can override.")

        live_colls = fetch_all_live_collections()
        if not live_colls:
            st.info("No live collections found. Create some first, or check your Shopify credentials.")
            live_colls = [(n, n.lower().replace(" ", "-")) for _, n, _, _, _, _ in COLLECTIONS_PLAN]

        coll_names   = [t for t, _ in live_colls]
        # Pre-select collections that match the topic keywords
        topic_words  = (topic + " " + keyword).lower().split() if topic else []
        preselected  = [i for i, (t, _) in enumerate(live_colls)
                        if any(w in t.lower() for w in topic_words)][:6]

        selected_idxs = st.multiselect(
            "Collections (pre-selected by topic relevance - adjust as needed)",
            options=list(range(len(live_colls))),
            default=preselected,
            format_func=lambda i: coll_names[i],
        )
        selected_colls = [live_colls[i] for i in selected_idxs]

    # ── Products to feature ──────────────────────────────────────────────────
    with st.expander("3. Products to Feature", expanded=False):
        st.caption("Search for specific products to call out in the post with real links and prices.")
        prod_query = st.text_input("Search products",
                                    placeholder="e.g. Harry Potter wand",
                                    key="prod_search")
        products = []
        if prod_query:
            products = search_shopify_products(prod_query)
            if products:
                prod_names    = [p["title"] for p in products]
                selected_prods = st.multiselect("Select products to feature",
                                                 options=list(range(len(products))),
                                                 default=list(range(min(4, len(products)))),
                                                 format_func=lambda i: prod_names[i])
                products = [products[i] for i in selected_prods]
                for p in products:
                    price = p["variants"][0]["price"] if p.get("variants") else ""
                    st.markdown(
                        f"- **{p['title']}** - £{price} - "
                        f"[{STORE_DOMAIN}/products/{p['handle']}]"
                        f"({STORE_DOMAIN}/products/{p['handle']})"
                    )
            else:
                st.info("No products found for that search.")

    # ── Generate ─────────────────────────────────────────────────────────────
    st.divider()
    if st.button("✍️ Generate Blog Post", type="primary",
                 use_container_width=True, disabled=not topic):
        if not topic or not keyword:
            st.warning("Fill in the topic and primary keyword first.")
        else:
            with st.spinner("Writing your post... (usually 20–40 seconds)"):
                prompt   = build_blog_prompt(topic, post_type, keyword, word_count,
                                             selected_colls, products)
                raw_text = call_anthropic(prompt)
            st.session_state["blog_output"]       = raw_text
            st.session_state["blog_topic"]        = topic
            st.session_state["blog_keyword"]      = keyword
            st.session_state["blog_html"]         = markdown_to_html(raw_text)
            st.session_state["blog_colls_used"]   = selected_colls

    # ── Output ───────────────────────────────────────────────────────────────
    if "blog_output" in st.session_state:
        raw  = st.session_state["blog_output"]
        html = st.session_state["blog_html"]

        # Extract metadata section
        meta_section = ""
        if "**Primary keyword:**" in raw or "Primary keyword:" in raw:
            parts = re.split(r"---\nSEO METADATA|SEO METADATA|---$", raw, flags=re.MULTILINE)
            if len(parts) > 1:
                meta_section = parts[-1].strip()
                raw = parts[0].strip()

        tab_prev, tab_html, tab_meta = st.tabs(["Preview", "HTML (for Shopify)", "SEO Metadata"])

        with tab_prev:
            st.markdown(raw)
            st.download_button("Download as Markdown", raw,
                                file_name=f"blog_{st.session_state['blog_keyword'].replace(' ','_')}.md",
                                mime="text/markdown", use_container_width=True)

        with tab_html:
            st.caption("Paste this directly into the Shopify blog editor (HTML mode).")
            st.code(html, language="html")
            st.download_button("Download HTML", html,
                                file_name=f"blog_{st.session_state['blog_keyword'].replace(' ','_')}.html",
                                mime="text/html", use_container_width=True)

        with tab_meta:
            if meta_section:
                st.markdown(meta_section)
            else:
                st.info("Metadata will appear here - it's usually at the bottom of the Preview tab.")

        # ── Publish to Shopify ────────────────────────────────────────────
        st.divider()
        st.subheader("Publish to Shopify Blog")
        blogs = fetch_shopify_blogs()
        if not blogs:
            st.info("No blogs found in your Shopify store. Create one in Shopify admin first.")
        else:
            blog_options = {title: bid for bid, title in blogs}
            chosen_blog  = st.selectbox("Select blog", list(blog_options.keys()))
            pub_tags     = st.text_input("Article tags (comma-separated)",
                                          value=st.session_state.get("blog_keyword", ""))

            if st.button("Publish to Shopify", type="primary", use_container_width=True):
                with st.spinner("Publishing..."):
                    res = publish_article_to_shopify(
                        blog_options[chosen_blog],
                        st.session_state["blog_topic"],
                        html,
                        "",
                        pub_tags,
                    )
                if "article" in res:
                    art = res["article"]
                    st.success(f"✅ Published: '{art['title']}'")
                    st.markdown(
                        f"[View in Shopify admin](https://{SHOPIFY_STORE}/admin/blogs/"
                        f"{blog_options[chosen_blog]}/articles/{art['id']})"
                    )
                else:
                    st.error(f"Publish failed: {res.get('errors', res)}")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: AUTO-TAGGER
# ══════════════════════════════════════════════════════════════════════════════
def page_tagger():
    st.header("Product Auto-Tagger")
    st.caption("Run the AI tagger on your catalogue. Last run: 7,952 / 7,973 products (99.7%).")

    if not os.path.exists(TAGGER_SCRIPT):
        st.warning(f"Tagger script not found. Make sure `infinity_tagger.py` is in "
                   f"the same folder as `app.py`.")
        return

    mode  = st.radio("Mode", ["DRY RUN (preview, no changes)",
                               "LIVE (tags applied to Shopify)"],
                     help="Always dry-run first to check the output.")
    batch = st.slider("Batch size", 1, 50, 10)
    live_flag = "--live" if "LIVE" in mode else ""

    if st.button("▶ Run Tagger", type="primary", use_container_width=True):
        st.warning("Running... do not close this tab. A full run takes ~2–3 hours.")
        cmd = [sys.executable, TAGGER_SCRIPT, "--batch-size", str(batch)]
        if live_flag:
            cmd.append("--live")
        log_area = st.empty()
        logs = []
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True,
                                    cwd=os.path.dirname(TAGGER_SCRIPT))
            for line in proc.stdout:
                logs.append(line.rstrip())
                log_area.code("\n".join(logs[-50:]), language="bash")
            proc.wait()
            if proc.returncode == 0:
                st.success("✅ Tagger run complete!")
            else:
                st.error(f"Exited with code {proc.returncode}")
        except Exception as e:
            st.error(f"Could not start tagger: {e}")

    st.divider()
    st.subheader("Previous Run Reports")
    outputs_dir = os.path.join(os.path.dirname(TAGGER_SCRIPT), "outputs")
    if os.path.isdir(outputs_dir):
        files = sorted(os.listdir(outputs_dir), reverse=True)
        report_files = [f for f in files if f.endswith(".csv") or f.endswith(".txt")]
        if report_files:
            chosen = st.selectbox("View file", report_files)
            fpath  = os.path.join(outputs_dir, chosen)
            if chosen.endswith(".csv"):
                df = pd.read_csv(fpath)
                st.dataframe(df, use_container_width=True)
                st.download_button("Download", df.to_csv(index=False),
                                    file_name=chosen, mime="text/csv",
                                    use_container_width=True)
            else:
                st.code(open(fpath).read(), language="bash")
        else:
            st.info("No reports yet.")
    else:
        st.info("No outputs folder found - run the tagger first.")


# ═════════════════════════════════════════════════════════════════════
# PAGE: TAG BROWSER
# ══════════════════════════════════════════════════════════════════════════════
def _load_tag_csvs():
    """Return (current_df, previous_df | None) from the two most recent tag CSVs."""
    out = os.path.join(os.path.dirname(__file__), "outputs")
    if not os.path.isdir(out):
        return None, None
    hits = sorted(
        [f for f in os.listdir(out) if f.startswith("tag_summary") and f.endswith(".csv")],
        reverse=True,
    )
    def _read(fname):
        df = pd.read_csv(os.path.join(out, fname))
        df.columns = ["Tag", "Products"]
        df["Products"] = pd.to_numeric(df["Products"], errors="coerce").fillna(0).astype(int)
        return df
    current  = _read(hits[0]) if len(hits) >= 1 else None
    previous = _read(hits[1]) if len(hits) >= 2 else None
    return current, previous


def _new_tags_section(current_df, previous_df, existing_colls, min_products=3):
    """Render the 'New Since Last Run' suggested collections panel."""
    if previous_df is None:
        return  # Only one run so far - nothing to compare against

    prev_tags = set(previous_df["Tag"].str.lower())
    new = current_df[
        (~current_df["Tag"].str.lower().isin(prev_tags)) &
        (current_df["Products"] >= min_products)
    ].sort_values("Products", ascending=False)

    if new.empty:
        return

    st.subheader(f"🆕 New Since Last Run  ·  {len(new)} tags")
    st.caption(
        f"These tags didn't exist in the previous tagger run and have ≥{min_products} active products. "
        "Create a collection in one click - SEO content is auto-generated."
    )

    for _, row in new.iterrows():
        tag  = row["Tag"]
        cnt  = int(row["Products"])
        live = tag.lower() in existing_colls
        c1, c2, c3 = st.columns([4, 1, 2])
        c1.markdown(f"**{tag}**")
        c2.markdown(f"`{cnt:,}`")
        if live:
            c3.success("Live ✅")
        else:
            if c3.button("Create Collection", key=f"new_{tag}", use_container_width=True):
                if not ACCESS_TOKEN or not SHOPIFY_STORE:
                    st.warning("Shopify credentials not found.")
                else:
                    with st.spinner(f"Creating '{tag}'..."):
                        seo = generate_collection_content(tag, tag)
                        res = create_smart_collection(tag, tag,
                                                       body_html=seo["body_html"],
                                                       seo_title=seo["seo_title"],
                                                       seo_description=seo["seo_description"])
                    if "smart_collection" in res:
                        st.toast(f"'{tag}' created!", icon="🎉")
                        fetch_existing_collections.clear()
                        st.rerun()
                    else:
                        st.error(str(res.get("errors", res)))

    # Bulk create all unbuilt new tags
    unbuild = new[~new["Tag"].str.lower().isin(existing_colls)]
    if not unbuild.empty and ACCESS_TOKEN and SHOPIFY_STORE:
        st.divider()
        if st.button(f"🚀 Create All {len(unbuild)} New Collections",
                     type="primary", use_container_width=True):
            prog = st.progress(0)
            ok = 0
            for i, (_, row) in enumerate(unbuild.iterrows()):
                seo = generate_collection_content(row["Tag"], row["Tag"])
                res = create_smart_collection(row["Tag"], row["Tag"],
                                               body_html=seo["body_html"],
                                               seo_title=seo["seo_title"],
                                               seo_description=seo["seo_description"])
                if "smart_collection" in res:
                    ok += 1
                prog.progress((i + 1) / len(unbuild))
                time.sleep(0.3)
            st.success(f"Done - {ok}/{len(unbuild)} collections created.")
            fetch_existing_collections.clear()
            st.rerun()

    st.divider()


def page_tags():
    st.header("Tag Browser")

    current_df, previous_df = _load_tag_csvs()

    if current_df is None:
        st.info("Tag summary CSV not found. Run the tagger first.")
        return

    total = len(current_df)
    runs_label = "comparing last 2 runs" if previous_df is not None else "first run on record"
    st.caption(f"Explore all {total:,} tags from your last tagger run ({runs_label}).")

    existing_colls = fetch_existing_collections()

    # ── New tags panel ────────────────────────────────────────────────────────
    _new_tags_section(current_df, previous_df, existing_colls)

    # ── Full tag browser ──────────────────────────────────────────────────────
    st.subheader("All Tags")
    search    = st.text_input("Search", placeholder="e.g. Liverpool, Harry Potter, T-Shirt")
    min_count = st.slider("Min product count", 1, 200, 1)

    filtered = current_df[current_df["Products"] >= min_count]
    if search:
        filtered = filtered[filtered["Tag"].str.contains(search, case=False, na=False)]

    st.metric("Tags shown", len(filtered), f"of {total:,} total")
    st.dataframe(filtered.head(200), use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Quick Create Collection from Tag")
    if not filtered.empty:
        sel_tag   = st.selectbox("Tag", filtered["Tag"].tolist())
        coll_name = st.text_input("Collection name", value=sel_tag)
        if st.button("Create Collection", type="primary", use_container_width=True):
            if not ACCESS_TOKEN or not SHOPIFY_STORE:
                st.warning("Shopify credentials not found.")
            else:
                with st.spinner():
                    seo = generate_collection_content(coll_name, sel_tag)
                    res = create_smart_collection(coll_name, sel_tag,
                                                   body_html=seo["body_html"],
                                                   seo_title=seo["seo_title"],
                                                   seo_description=seo["seo_description"])
                if "smart_collection" in res:
                    st.success(f"✅ '{coll_name}' created!")
                    fetch_existing_collections.clear()
                else:
                    st.error(str(res.get("errors", res)))


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    st.set_page_config(
        page_title="Infinity Collectables – Ops",
        page_icon="♾️",
        layout="wide",
        # Collapsed by default so mobile gets full screen immediately
        initial_sidebar_state="collapsed",
    )

    # Inject mobile-friendly CSS
    st.markdown("""
    <style>
    /* Tighter padding on mobile */
    .block-container { padding: 1rem 1rem 2rem 1rem !important; }
    /* Bigger tap targets */
    .stButton button { min-height: 44px; font-size: 15px; }
    .stSelectbox > div, .stTextInput > div > input { font-size: 16px; }
    /* Keep sidebar nav readable */
    [data-testid="stSidebarNav"] { font-size: 15px; }
    </style>
    """, unsafe_allow_html=True)

    if not _check_password():
        st.stop()

    st.sidebar.markdown(
        "<h3 style='color:#0D1B2A;margin-bottom:0'>♾️ Infinity</h3>"
        "<p style='color:#888;margin-top:0;font-size:13px'>Ops Dashboard</p>",
        unsafe_allow_html=True)
    st.sidebar.divider()

    page = st.sidebar.radio("", [
        "📋 Collections",
        "➕ Create Collection",
        "✍️ Blog Writer",
        "🤖 Auto-Tagger",
        "🏷️ Tag Browser",
    ], label_visibility="collapsed")

    st.sidebar.divider()
    st.sidebar.caption(
        f"{'✅' if ACCESS_TOKEN else '❌'} Shopify  \n"
        f"{'✅' if ANTHROPIC_KEY else '❌'} Anthropic AI"
    )

    if page == "📋 Collections":
        page_dashboard()
    elif page == "➕ Create Collection":
        page_creator()
    elif page == "✍️ Blog Writer":
        page_blog()
    elif page == "🤖 Auto-Tagger":
        page_tagger()
    elif page == "🏷️ Tag Browser":
        page_tags()


if __name__ == "__main__":
    main()
