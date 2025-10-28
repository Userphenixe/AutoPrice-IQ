import asyncio, re
from urllib.parse import urljoin
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

BASE = "https://www.leboncoin.fr/c/voitures"
NB_PAGES = 10
NBSP = "\u00A0"

# Regex robuste pour supprimer les emojis et pictogrammes
EMOJI_RE = re.compile(
    "["                             # blocs emoji/pictos fréquents
    "\U0001F1E6-\U0001F1FF"         # drapeaux
    "\U0001F300-\U0001F5FF"         # symboles & pictogrammes
    "\U0001F600-\U0001F64F"         # émoticônes
    "\U0001F680-\U0001F6FF"         # transport & cartes
    "\U0001F700-\U0001F77F"
    "\U0001F780-\U0001F7FF"
    "\U0001F800-\U0001F8FF"
    "\U0001F900-\U0001F9FF"
    "\U0001FA00-\U0001FAFF"
    "\u2600-\u26FF"                 # symboles divers (soleil, etc.)
    "\u2700-\u27BF"                 # dingbats
    "]",
    flags=re.UNICODE
)

def clean(s: str | None) -> str:
    if not s: return ""
    s = s.replace(NBSP, " ").replace("\n", " ")
    return re.sub(r"\s+", " ", s).strip()

def strip_emojis(s: str | None) -> str:
    if not s: return ""
    return EMOJI_RE.sub("", s).strip()

def price_to_int(txt: str | None):
    if not txt: return None
    digits = re.sub(r"[^\d]", "", txt)
    return int(digits) if digits else None

def parse_params(raw: str | None):
    # "2018 · 139500 km · Essence · Manuelle"
    if not raw: return "", "", "", ""
    parts = [clean(p) for p in re.split(r"\s*[·|\|]\s*", raw) if p.strip()]
    while len(parts) < 4: parts.append("")
    year, km, fuel, box = parts[:4]
    year = re.search(r"\d{4}", year).group(0) if re.search(r"\d{4}", year) else ""
    km   = re.sub(r"[^\d]", "", km)
    return year, km, fuel, box

async def handle_cookies(page):
    for label in ["Tout accepter","Accepter","Continuer sans accepter","J'accepte","Je comprends"]:
        try:
            await page.get_by_role("button", name=label).click(timeout=700)
            return
        except Exception:
            pass
    try:
        for f in page.context.pages[0].frames:
            if (f.name() or "").startswith("sp_message_iframe") or "sp_message_iframe" in (f.url or ""):
                for label in ["Tout accepter","Accepter","Continuer sans accepter"]:
                    try:
                        await f.get_by_role("button", name=label).click(timeout=700)
                        return
                    except Exception:
                        continue
    except Exception:
        pass

async def wait_for_cards(page):
    sels = [
        'article[data-qa-id="aditem_container"]',
        '[data-test-id="adcard-title"]',
        'li[class*="styles_adCard"] article',
    ]
    for _ in range(6):
        for sel in sels:
            try:
                await page.wait_for_selector(sel, state="attached", timeout=60000)
                return
            except PWTimeout:
                continue
        await page.mouse.wheel(0, 1800)
        await asyncio.sleep(0.8)
    raise TimeoutError("Cartes d’annonces introuvables (cookies/lazy-load).")

async def location_from_card(card):
    try:
        for t in await card.locator("p.sr-only").all_inner_texts():
            m = re.search(r"Située?\s+à\s+(.+?)\.", t)
            if m: return clean(m.group(1))
    except Exception:
        pass
    try:
        p = card.locator("p.text-caption.text-neutral").first
        txt = await p.inner_text(timeout=500)
        if txt: return clean(txt)
    except Exception:
        pass
    return ""

async def scrape_list_page(page, page_num: int):
    url = f"{BASE}?page={page_num}"
    await page.goto(url, wait_until="domcontentloaded")
    await handle_cookies(page)
    await page.wait_for_load_state("networkidle", timeout=60000)
    # petit scroll de chauffe
    await page.mouse.wheel(0, 1200)
    await asyncio.sleep(0.2)

    await wait_for_cards(page)

    cards = page.locator('article[data-qa-id="aditem_container"]')
    if await cards.count() == 0:
        cards = page.locator('li[class*="styles_adCard"] article')

    rows = []
    n = await cards.count()
    for i in range(n):
        card = cards.nth(i)

        # titre
        title = ""
        for sel in ['[data-test-id="adcard-title"]', 'h3']:
            try:
                title = clean(await card.locator(sel).first.inner_text(timeout=600))
                if title:
                    title = strip_emojis(title)
                    break
            except Exception:
                pass

        # prix
        price_txt = ""
        try:
            price_txt = clean(await card.locator('[data-test-id="price"]').first.inner_text(timeout=600))
        except Exception:
            pass
        price = price_to_int(price_txt)

        # paramètres
        params_txt = ""
        for sel in ['[data-test-id="ad-params-light"]', '[data-test-id="ad-params-labels"]', '.text-body-2']:
            try:
                params_txt = clean(await card.locator(sel).first.inner_text(timeout=600))
                if re.search(r"\d{4}", params_txt): break
            except Exception:
                pass
        year, km, fuel, gearbox = parse_params(params_txt)

        # lieu
        location = await location_from_card(card)

        rows.append({
            "title": title,
            "price_eur": price,
            "year": year,
            "kilometers": km,
            "fuel": fuel,
            "gearbox": gearbox,
            "location": location
        })
    return rows

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled","--no-sandbox"]
        )
        context = await browser.new_context(
            locale="fr-FR",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"),
            extra_http_headers={"Accept-Language":"fr-FR,fr;q=0.9,en;q=0.8"},
            viewport={"width":1366,"height":768},
        )
        page = await context.new_page()

        for page_num in range(1, NB_PAGES+1):
            print(f"\n===== Page {page_num} =====")
            try:
                rows = await scrape_list_page(page, page_num)
            except Exception as e:
                print(f"[WARN] Page {page_num}: {e}")
                continue

            for r in rows:
                print(f"- {r['title']} | {r['price_eur']} € | {r['year']} | "
                      f"{r['kilometers']} km | {r['fuel']} | {r['gearbox']} | "
                      f"{r['location']}")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
