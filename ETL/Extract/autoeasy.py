import asyncio
import re
from urllib.parse import urljoin
from pathlib import Path

from playwright.async_api import async_playwright, TimeoutError as PWTimeout
import pandas as pd

BASE = "https://www.autoeasy.fr/"
NBSP = "\u00A0"

# .../AUTOPRICE-IQ/ETL
BASE_DIR = Path(__file__).resolve().parents[1]
# .../AUTOPRICE-IQ/ETL/data/autoeasy.csv
DEFAULT_DEST_FILE = BASE_DIR / "data" / "autoeasy.csv"

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


# ================== HELPERS PLAYWRIGHT ==================

async def handle_cookies(page):
    for label in [
        "Tout accepter", "J'accepte", "Accepter", "Continuer sans accepter",
        "Je comprends", "OK"
    ]:
        try:
            await page.get_by_role("button", name=label).click(timeout=700)
            return
        except Exception:
            pass

    try:
        for f in page.frames:
            name_url = (f.name() or "") + " " + (f.url or "")
            if "sp_message_iframe" in name_url or "privacy" in name_url.lower():
                for label in ["Tout accepter", "Accepter", "Continuer sans accepter"]:
                    try:
                        await f.get_by_role("button", name=label).click(timeout=700)
                        return
                    except Exception:
                        continue
    except Exception:
        pass


async def wait_for_carousel(page):
    sels = [
        "#homefeatured .slick-track > li",
        "ul.product_list.slick-slider .slick-track > li",
        "ul#homefeatured .slick-list",
    ]
    for _ in range(8):
        for sel in sels:
            try:
                await page.wait_for_selector(sel, state="attached", timeout=10000)
                return
            except PWTimeout:
                continue
        await page.mouse.wheel(0, 1200)
        await asyncio.sleep(0.6)
    raise TimeoutError("Carrousel introuvable (cookies/lazy-load?)")


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


async def scrape_autoeasy_home(page):
    await page.goto(BASE, wait_until="domcontentloaded")
    await handle_cookies(page)
    await page.wait_for_load_state("networkidle", timeout=60000)

    await page.mouse.wheel(0, 1200)
    await asyncio.sleep(0.3)

    await wait_for_carousel(page)

    slides = page.locator('#homefeatured .slick-track > li:not(.slick-cloned)')
    if await slides.count() == 0:
        slides = page.locator(
            "ul.product_list.slick-slider .slick-track > li:not(.slick-cloned)"
        )

    n = await slides.count()
    if n == 0:
        return await scrape_by_clicking(page)

    rows = []
    for i in range(n):
        li = slides.nth(i)

        title = strip_emojis(await _txt(li, "a.product-name"))
        href = await _attr(li, "a.product-name", "href")

        year = clean(await _txt(li, "p.product-caracteristic span.year"))
        year = re.search(r"\d{4}", year).group(0) if re.search(r"\d{4}", year) else ""
        km = clean(await _txt(li, "p.product-caracteristic span.km"))
        km = re.sub(r"[^\d]", "", km) if km else ""
        fuel = clean(await _txt(li, "p.product-caracteristic span.motor"))
        gearbox = clean(await _txt(li, "p.product-caracteristic span.boite"))

        price_meta = await _attr(li, 'span[itemprop="price"]', "content")
        price_txt = await _txt(li, "span.price")
        price_eur = price_to_int(price_meta or price_txt)

        rows.append(
            {
                "title": title,
                "price_eur": price_eur,
                "year": year,
                "kilometers": km,
                "fuel": fuel,
                "gearbox": gearbox,
            }
        )
    return rows


async def scrape_by_clicking(page, max_turns: int = 200):
    container = page.locator("#homefeatured").first
    next_btn = page.locator("button.slick-next").first
    track = page.locator("#homefeatured .slick-track").first

    await page.wait_for_timeout(400)
    seen_ids = set()
    rows = []

    for i in range(max_turns):
        current = track.locator('> li[aria-hidden="false"]:not(.slick-cloned)').first
        if await current.count() == 0:
            current = track.locator("> li:not(.slick-cloned)").first

        sid = (
            await current.get_attribute("id")
            or await current.get_attribute("data-slick-index")
            or str(i)
        )
        if sid in seen_ids:
            break
        seen_ids.add(sid)

        title = strip_emojis(await _txt(current, "a.product-name"))
        href = await _attr(current, "a.product-name", "href")

        year = clean(await _txt(current, "p.product-caracteristic span.year"))
        year = re.search(r"\d{4}", year).group(0) if re.search(r"\d{4}", year) else ""
        km = clean(await _txt(current, "p.product-caracteristic span.km"))
        km = re.sub(r"[^\d]", "", km) if km else ""
        fuel = clean(await _txt(current, "p.product-caracteristic span.motor"))
        gearbox = clean(await _txt(current, "p.product-caracteristic span.boite"))

        price_meta = await _attr(current, 'span[itemprop="price"]', "content")
        price_txt = await _txt(current, "span.price")
        price_eur = price_to_int(price_meta or price_txt)

        rows.append(
            {
                "title": title,
                "price_eur": price_eur,
                "year": year,
                "kilometers": km,
                "fuel": fuel,
                "gearbox": gearbox,
            }
        )

        try:
            await next_btn.click(timeout=2000)
        except Exception:
            await page.evaluate(
                """btn => btn && btn.click()""", await next_btn.element_handle()
            )
        await asyncio.sleep(0.25)

    return rows



def save_rows_to_csv(rows: list[dict], dest_file: Path):
    df = pd.DataFrame(rows)
    dest_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(dest_file, index=True, index_label="id")
    print(f"File Loaded Successfully to {dest_file} !")



async def _async_scrape_autoeasy() -> list[dict]:
    """
    Fonction async centrale : ouvre le navigateur, scrape le carrousel
    et renvoie toutes les lignes.
    """
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

        print("===== Autoeasy — Carrousel d’annonces en page d’accueil =====")
        try:
            rows = await scrape_autoeasy_home(page)
        except Exception as e:
            print(f"[WARN] Fallback par clic: {e}")
            rows = await scrape_by_clicking(page)

        for r in rows:
            print(
                f"- {r['title']} | {r['price_eur']} | {r['year']} | "
                f"{r['kilometers']} km | {r['fuel']} | {r['gearbox']}"
            )

        await browser.close()
        return rows


def run_autoeasy(dest_file: str | Path | None = None) -> Path:
    """
    Point d’entrée synchrone à utiliser depuis Airflow ou en local.

    - dest_file : chemin du CSV, par défaut DEFAULT_DEST_FILE
    Retourne le Path du fichier créé.
    """
    dest_path = Path(dest_file) if dest_file is not None else DEFAULT_DEST_FILE
    rows = asyncio.run(_async_scrape_autoeasy())
    save_rows_to_csv(rows, dest_path)
    return dest_path

if __name__ == "__main__":
    run_autoeasy()
