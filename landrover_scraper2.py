import csv
import os
import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
BASE_URL = "https://accessories.landrover.com/br/pt/"
DELAY = 0.3

BUBBLE_APP_NAME  = os.environ.get("BUBBLE_APP_NAME", "")
BUBBLE_API_TOKEN = os.environ.get("BUBBLE_API_TOKEN", "")
BUBBLE_DATA_TYPE = os.environ.get("BUBBLE_DATA_TYPE", "")

BUBBLE_URL = f"https://{BUBBLE_APP_NAME}.bubbleapps.io/api/1.1/obj/{BUBBLE_DATA_TYPE}"

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
    return (
        str(value)
        .replace("\n", "")
        .replace("\r", "")
        .replace("\t", "")
        .strip()
    )

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

    for a in soup.find_all("a", href=True):
        if "brand=" in a["href"]:
            qs = parse_qs(urlparse(a["href"]).query)
            brand = qs.get("brand", [None])[0]
            if brand and brand not in [b["brand"] for b in brands]:
                brands.append({
                    "familia": brand,
                    "brand": brand,
                    "url": urljoin(BASE_URL, a["href"])
                })

    return brands


def get_models(brand, session):
    soup = get_soup(brand["url"], session)
    models = []

    for a in soup.find_all("a", href=True):
        parsed = urlparse(a["href"])
        parts = parsed.path.strip("/").split("/")

        if len(parts) == 3:
            modelo = parts[2]
            if modelo not in [m["modelo"] for m in models]:
                models.append({
                    "modelo": modelo,
                    "url": urljoin(BASE_URL, a["href"])
                })

    return models


def get_products(model_url, session):
    urls = []
    page = 1

    while True:
        url = f"{model_url}?page={page}"
        soup = get_soup(url, session)
        if not soup:
            break

        found = 0
        for a in soup.select("a[href*='/br/pt/']"):
            href = a.get("href")
            if href and href not in urls:
                urls.append(urljoin(BASE_URL, href))
                found += 1

        if found == 0:
            break

        page += 1
        time.sleep(DELAY)

    return urls


def scrape_product(url, familia, modelo, session):
    soup = get_soup(url, session)
    if not soup:
        return None

    codigo = None
    for strong in soup.find_all("strong"):
        if "acessório" in strong.text.lower():
            match = re.search(r"([A-Z0-9]{5,})", strong.parent.text)
            if match:
                codigo = match.group(1)
                break

    img = soup.find("img")
    imagem_url = img["src"] if img and img.get("src") else ""

    return {
        "familia": clean(familia),
        "modelo": clean(modelo),
        "codigo": clean(codigo),
        "imagem_url": clean(imagem_url),
    }

# ---------------------------------------------------------------------------
# BUBBLE
# ---------------------------------------------------------------------------

def exists_in_bubble(codigo):
    if not codigo:
        return False

    url = f"{BUBBLE_URL}?constraints=[{{\"key\":\"codigo\",\"constraint_type\":\"equals\",\"value\":\"{codigo}\"}}]"
    r = requests.get(url, headers=BUBBLE_HEADERS)

    if r.status_code == 200:
        return len(r.json()["response"]["results"]) > 0

    return False


def send_to_bubble(row):
    for _ in range(3):  # retry
        try:
            r = requests.post(BUBBLE_URL, json=row, headers=BUBBLE_HEADERS, timeout=10)
            if r.status_code in [200, 201]:
                return True
        except:
            time.sleep(1)
    return False


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    session = requests.Session()

    with open("output.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["familia", "modelo", "codigo", "imagem_url"],
            delimiter=";",
            quoting=csv.QUOTE_ALL
        )
        writer.writeheader()

        total = 0

        for brand in get_brands(session):
            print(f"\n=== {brand['familia']} ===")

            for model in get_models(brand, session):
                print(f"  -> {model['modelo']}")

                products = get_products(model["url"], session)

                for url in products:
                    row = scrape_product(url, brand["familia"], model["modelo"], session)

                    if not row or not row["codigo"]:
                        continue

                    if exists_in_bubble(row["codigo"]):
                        print(f"    SKIP {row['codigo']}")
                        continue

                    writer.writerow(row)
                    f.flush()

                    ok = send_to_bubble(row)

                    print(f"    {'OK' if ok else 'ERRO'} - {row['codigo']}")

                    total += 1
                    time.sleep(DELAY)

    print(f"\n✅ FINALIZADO - {total} itens")


if __name__ == "__main__":
    main()
