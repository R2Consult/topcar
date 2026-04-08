import csv
import json
import os
import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs, urlencode

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
BASE_URL = "https://accessories.landrover.com/br/pt/"
DELAY = 0.3

BUBBLE_APP_NAME  = os.environ.get("BUBBLE_APP_NAME", "")
BUBBLE_API_TOKEN = os.environ.get("BUBBLE_API_TOKEN", "")
BUBBLE_DATA_TYPE = os.environ.get("BUBBLE_DATA_TYPE", "")

BUBBLE_URL = (
    f"https://{BUBBLE_APP_NAME}.bubbleapps.io/api/1.1/obj/{BUBBLE_DATA_TYPE}"
    if BUBBLE_APP_NAME and BUBBLE_DATA_TYPE else ""
)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "pt-BR,pt;q=0.9",
}

BUBBLE_HEADERS = {
    "Authorization": f"Bearer {BUBBLE_API_TOKEN}",
    "Content-Type": "application/json",
}

# ---------------------------------------------------------------------------
# UTIL
# ---------------------------------------------------------------------------

def clean(value):
    if not value:
        return ""
    return str(value).replace("\n", "").replace("\r", "").replace("\t", "").strip()

def get_soup(url, session):
    try:
        r = session.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except:
        return None

# ---------------------------------------------------------------------------
# SCRAPER
# ---------------------------------------------------------------------------

def get_brands(session):
    soup = get_soup(BASE_URL, session)
    brands = []
    if not soup:
        return brands

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "brand=" not in href:
            continue
        parsed = urlparse(href)
        parts = [p for p in parsed.path.strip("/").split("/") if p]
        if len(parts) > 2:
            continue
        qs = parse_qs(parsed.query)
        brand = qs.get("brand", [None])[0]
        if brand and brand not in [b["brand"] for b in brands]:
            brands.append({
                "familia": brand,
                "brand": brand,
                "url": urljoin(BASE_URL, href)
            })

    return brands


def get_models(brand, session):
    soup = get_soup(brand["url"], session)
    models = []
    if not soup:
        return models

    brand_val = brand["brand"]
    for a in soup.find_all("a", href=True):
        parsed = urlparse(a["href"])
        parts = [p for p in parsed.path.strip("/").split("/") if p]
        if len(parts) == 3 and parts[0] == "br" and parts[1] == "pt":
            modelo = parts[2]
            qs = parse_qs(parsed.query)
            if qs.get("brand", [None])[0] == brand_val:
                if modelo not in [m["modelo"] for m in models]:
                    models.append({
                        "modelo": modelo,
                        "url": urljoin("https://accessories.landrover.com", a["href"])
                    })

    return models


def get_model_image(model_url, session):
    """Busca a imagem principal do modelo na página de listagem."""
    soup = get_soup(model_url, session)
    if not soup:
        return ""

    # Prioridade 1: og:image (meta tag — imagem principal do modelo)
    og = soup.find("meta", {"property": "og:image"})
    if og and og.get("content"):
        return og["content"]

    # Prioridade 2: banner/hero da página
    img = soup.select_one("section.banner-stealth img, section[class*='banner'] img")
    if img:
        src = img.get("src") or img.get("data-src")
        if src:
            return urljoin("https://accessories.landrover.com", src)

    # Prioridade 3: primeira img de assets do veículo (não de acessório)
    for img in soup.find_all("img", src=True):
        src = img["src"]
        if "assets.config.landrover.com" in src and "/accessories/" not in src:
            return src

    return ""


def get_products(model_url, session):
    urls = set()
    page = 1

    while True:
        sep = "&" if "?" in model_url else "?"
        url = f"{model_url}{sep}page={page}"
        soup = get_soup(url, session)
        if not soup:
            break

        found = 0
        for a in soup.select("a[href*='/br/pt/']"):
            href = a.get("href", "")
            # remove fragmento #tab-section para evitar duplicatas
            clean_href = href.split("#")[0]
            full = urljoin("https://accessories.landrover.com", clean_href)
            if full not in urls:
                urls.add(full)
                found += 1

        if found == 0:
            break

        page += 1
        time.sleep(DELAY)

    return list(urls)


def scrape_product(url, familia, modelo, model_image, session):
    soup = get_soup(url, session)
    if not soup:
        return None

    # Código do acessório
    codigo = None
    for strong in soup.find_all("strong"):
        if "acessório" in strong.text.lower() or "acessorio" in strong.text.lower():
            match = re.search(r"([A-Z0-9]{5,})", strong.parent.get_text())
            if match:
                codigo = match.group(1)
                break

    # Imagem do acessório
    imagem_url = ""

    # Prioridade 1: data-testid com código do acessório
    if codigo:
        img_el = soup.find("img", {"data-testid": f"{codigo}_feature_img"})
        if img_el:
            imagem_url = img_el.get("src") or img_el.get("data-src") or ""

    # Prioridade 2: qualquer data-testid terminando em _feature_img
    if not imagem_url:
        img_el = soup.find("img", {"data-testid": re.compile(r"_feature_img$")})
        if img_el:
            imagem_url = img_el.get("src") or img_el.get("data-src") or ""

    # Prioridade 3: img em assets.config.landrover.com/accessories/
    if not imagem_url:
        for img in soup.find_all("img", src=True):
            src = img["src"]
            if "assets.config.landrover.com" in src and "/accessories/" in src:
                imagem_url = src
                break

    # Prioridade 4: lightbox
    if not imagem_url:
        lb_img = soup.select_one("section.lightbox ol li img")
        if lb_img:
            src = lb_img.get("src") or lb_img.get("data-src") or ""
            if src:
                imagem_url = urljoin("https://accessories.landrover.com", src)

    return {
        "familia":         clean(familia),
        "modelo":          clean(modelo),
        "model_image_url": clean(model_image),
        "codigo":          clean(codigo),
        "imagem_url":      clean(imagem_url),
    }

# ---------------------------------------------------------------------------
# BUBBLE
# ---------------------------------------------------------------------------

def exists_in_bubble(codigo):
    """Verifica se o código já existe no Bubble usando constraints corretamente."""
    if not codigo or not BUBBLE_URL:
        return False

    # constraints deve ser JSON serializado e passado como parâmetro
    constraints = json.dumps([{
        "key": "codigo",
        "constraint_type": "equals",
        "value": codigo
    }])

    try:
        r = requests.get(
            BUBBLE_URL,
            params={"constraints": constraints},
            headers=BUBBLE_HEADERS,
            timeout=10
        )
        if r.status_code == 200:
            return r.json()["response"]["count"] > 0
    except:
        pass

    return False


def send_to_bubble(row):
    """Envia registro para o Bubble com retry automático."""
    if not BUBBLE_URL or not BUBBLE_API_TOKEN:
        return False

    payload = {
        "familia":         row["familia"],
        "modelo":          row["modelo"],
        "model_image_url": row["model_image_url"],
        "codigo":          row["codigo"],
        "imagem_url":      row["imagem_url"],
    }

    for attempt in range(3):
        try:
            r = requests.post(BUBBLE_URL, json=payload, headers=BUBBLE_HEADERS, timeout=10)
            if r.status_code in [200, 201]:
                return True
            print(f"      Bubble erro {r.status_code}: {r.text[:150]}")
        except Exception as e:
            print(f"      Bubble exceção: {e}")
        time.sleep(1)

    return False

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    session = requests.Session()
    bubble_ativo = bool(BUBBLE_URL and BUBBLE_API_TOKEN)

    print("🔗 Bubble ativo" if bubble_ativo else "⚠️  Bubble não configurado — apenas CSV")

    with open("output.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["familia", "modelo", "model_image_url", "codigo", "imagem_url"],
            delimiter=";",
            quoting=csv.QUOTE_ALL
        )
        writer.writeheader()

        total = 0

        for brand in get_brands(session):
            print(f"\n=== {brand['familia']} ===")
            time.sleep(DELAY)

            for model in get_models(brand, session):
                print(f"  -> {model['modelo']}")
                time.sleep(DELAY)

                model_image = get_model_image(model["url"], session)
                print(f"     model_image: {model_image or 'não encontrada'}")

                products = get_products(model["url"], session)
                print(f"     {len(products)} produtos encontrados")

                for url in products:
                    row = scrape_product(url, brand["familia"], model["modelo"], model_image, session)

                    if not row or not row["codigo"]:
                        continue

                    if bubble_ativo and exists_in_bubble(row["codigo"]):
                        print(f"    SKIP {row['codigo']}")
                        continue

                    writer.writerow(row)
                    f.flush()

                    if bubble_ativo:
                        ok = send_to_bubble(row)
                        print(f"    {'OK' if ok else 'ERRO'} - {row['codigo']}")
                    else:
                        print(f"    CSV - {row['codigo']}")

                    total += 1
                    time.sleep(DELAY)

    print(f"\n✅ FINALIZADO - {total} itens")


if __name__ == "__main__":
    main()
