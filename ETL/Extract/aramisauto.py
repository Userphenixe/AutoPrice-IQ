import asyncio
import re
from urllib.parse import urljoin
from pathlib import Path

from playwright.async_api import async_playwright, TimeoutError as PWTimeout
import pandas as pd

BASE = "https://www.aramisauto.com/achat/"
NB_PAGES = 10
NBSP = "\u00A0"

# .../AUTOPRICE-IQ/ETL
BASE_DIR = Path(__file__).resolve().parents[1]
# .../AUTOPRICE-IQ/ETL/data/aramisauto.csv
DEFAULT_DEST_FILE = BASE_DIR / "data" / "aramisauto.csv"

# ================== HELPERS TEXTE ==================

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
    "\u2600-\u26FF"                 # symboles divers
    "\u2700-\u27BF"                 # dingbats
    "]",
    flags=re.UNICODE,
)


def clean(s: str | None) -> str:
    if not s:
        return ""
    s = s.replace(NBSP, " ").replace("\n", " ")
    return re.sub(r"\s+", " ", s).strip()


def strip_emojis(s: str | None) -> str:
    if not s:
        return ""
    return EMOJI_RE.sub("", s).strip()


def price_to_int(txt: str | None):
    if not txt:
        return None
    digits = re.sub(r"[^\d]", "", txt)
    return int(digits) if digits else None


def parse_bottom(txt: str | None):
    """
    Exemples :
      - '2025 • 707 km • Occasion'
      - '2019 • 45 120 km • Occasion'
    """
    if not txt:
        return "", "", ""
    year = re.search(r"\b(19|20)\d{2}\b", txt)
    year = year.group(0) if year else ""
    km = re.search(r"([\d\s\u00A0]+)\s*km", txt, flags=re.I)
    km = re.sub(r"[^\d]", "", km.group(1)) if km else ""
    cond = "Occasion" if "occasion" in txt.lower() else (
        "Neuf" if "neuf" in txt.lower() else ""
    )
    return year, km, cond


def guess_fuel_details(txt: str | None):
    """
    Détails vus :
      - 'Hybrid 145 e-DC5G • Allure + Caméra'
      - 'Micro-hybride essence • Auto.'
    On renvoie (fuel, gearbox)
    """
    if not txt:
        return "", ""
    low = txt.lower()
    fuel = ""
    for key in ["électrique", "electrique", "essence", "diesel", "hybride", "micro-hybride", "gpl"]:
        if key in low:
            fuel = "Hybride" if "hybride" in key else key.capitalize()
            if fuel == "Electrique":
                fuel = "Électrique"
            break
    gearbox = "Automatique" if ("auto" in low or "automatique" in low) else (
        "Manuelle" if "manu" in low else ""
    )
    return fuel, gearbox


async def handle_cookies(page):
    for label in ["Tout accepter", "Accepter", "Continuer sans accepter",
                  "J'accepte", "Je comprends", "OK"]:
        try:
            await page.get_by_role("button", name=label).click(timeout=700)
            return
        except Exception:
            pass
    try:
        for f in page.frames:
            name_url = (f.name() or "") + " " + (f.url or "")
            if "sp_message_iframe" in name_url or "consent" in name_url.lower():
                for label in ["Tout accepter", "Accepter", "Continuer sans accepter"]:
                    try:
                        await f.get_by_role("button", name=label).click(timeout=700)
                        return
                    except Exception:
                        continue
    except Exception:
        pass


async def wait_for_cards(page):
    sels = [
        'div.results div.list > div.item a[href^="/voitures/"]',
        'div.list > div.item .product-card-content',
        'a.product-card',
    ]
    for _ in range(8):
        for sel in sels:
            try:
                await page.wait_for_selector(sel, state="attached", timeout=12000)
                return
            except PWTimeout:
                continue
        await page.mouse.wheel(0, 1600)
        await asyncio.sleep(0.8)
    raise TimeoutError("Cartes d’annonces introuvables (cookies/lazy-load).")


async def _txt(parent, sel):
    try:
        return clean(await parent.locator(sel).first.inner_text(timeout=800))
    except Exception:
        return ""


async def _attr(parent, sel, name):
    try:
        return await parent.locator(sel).first.get_attribute(name, timeout=800)
    except Exception:
        return None


async def _text_of_first(parent, selectors: list[str], timeout=1500) -> str:
    for sel in selectors:
        loc = parent.locator(sel).first
        try:
            await loc.wait_for(state="attached", timeout=timeout)
            txt = await loc.text_content(timeout=timeout)
            if txt:
                return clean(txt)
        except Exception:
            continue
    return ""


async def scrape_list_page(page, page_num: int):
    url = f"{BASE}?page={page_num}"
    await page.goto(url, wait_until="domcontentloaded")
    await handle_cookies(page)
    await page.wait_for_load_state("networkidle", timeout=60000)


    await page.mouse.wheel(0, 1600)
    await asyncio.sleep(0.4)

    await wait_for_cards(page)

    cards = page.locator("div.results div.list > div.item")
    if await cards.count() == 0:
        cards = page.locator("div.list > div.item")

    rows = []
    n = await cards.count()
    for i in range(n):
        item = cards.nth(i)

        try:
            await item.scroll_into_view_if_needed(timeout=1200)
            await asyncio.sleep(0.05)
        except Exception:
            pass

        # lien + titre
        href = await _attr(item, 'a[href^="/voitures/"]', "href")
        url_full = urljoin(BASE, href) if href else ""

        title = await _txt(item, "span.product-card-vehicle-information__title") \
            or await _txt(item, "h3")

        # details
        details_light = await _txt(item, "span.product-card-vehicle-information__details--light")
        details_medium = await _txt(
            item,
            "span.product-card-vehicle-information__details.text-xs--medium",
        )
        details = " • ".join([t for t in (details_light, details_medium) if t])

        bottom = await _txt(item, "span.product-card-vehicle-information__bottom")
        year, km, condition = parse_bottom(bottom)
        fuel, gearbox = guess_fuel_details(details)


        price_striked_txt = await _text_of_first(
            item,
            [
                "span.price-striked",
                "li.product-card-price__manufacturer-price .price-striked",
                "div.price-wrapper .price-striked",
            ],
            timeout=2500,
        )
        price_eur = price_to_int(price_striked_txt)

        rows.append(
            {
                "title": strip_emojis(title),
                "price_eur": price_eur,
                "currency": "EUR",
                "year": year,
                "kilometers": km,
                "fuel": fuel,
                "gearbox": gearbox,
                "condition": condition,
            }
        )

    return rows


def save_rows_to_csv(rows: list[dict], dest_file: Path):
    df = pd.DataFrame(rows)
    dest_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(dest_file, index=True, index_label="id")
    print(f"File Loaded Successfully to {dest_file} !")

async def _async_scrape_aramisauto(nb_pages: int) -> list[dict]:
    """Fonction async centrale : ouvre le navigateur, scrape nb_pages et renvoie les lignes."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        context = await browser.new_context(
            locale="fr-FR",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            extra_http_headers={"Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8"},
            viewport={"width": 1366, "height": 768},
        )
        page = await context.new_page()

        all_rows: list[dict] = []

        for page_num in range(1, nb_pages + 1):
            print(f"\n===== Page {page_num} =====")
            try:
                rows = await scrape_list_page(page, page_num)
            except Exception as e:
                print(f"[WARN] Page {page_num}: {e}")
                continue

            all_rows.extend(rows)

            for r in rows:
                print(
                    f"- {r['title']} | {r['price_eur']} € | {r['year']} | "
                    f"{r['kilometers']} km | {r['fuel']} | {r['gearbox']} | "
                    f"{r['condition']}"
                )

        await browser.close()
        return all_rows


def run_aramisauto(nb_pages: int = NB_PAGES, dest_file: str | Path | None = None) -> Path:
    """
    Point d'entrée synchrone à utiliser depuis Airflow ou en local.

    - nb_pages : nombre de pages à scraper
    - dest_file : chemin du CSV, par défaut DEFAULT_DEST_FILE
    Retourne le Path du fichier créé.
    """
    dest_path = Path(dest_file) if dest_file is not None else DEFAULT_DEST_FILE
    rows = asyncio.run(_async_scrape_aramisauto(nb_pages))
    save_rows_to_csv(rows, dest_path)
    return dest_path


if __name__ == "__main__":
    run_aramisauto()
