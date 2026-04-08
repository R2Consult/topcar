"""
Land Rover Accessories Scraper + Bubble.io Importer
Gera CSV com: familia, modelo, codigo, imagem_url
E envia cada registro direto para o Bubble via Data API.

Variáveis de ambiente necessárias:
  BUBBLE_APP_NAME  → nome do seu app (ex: meuapp)
  BUBBLE_API_TOKEN → token gerado em Settings > API no Bubble
  BUBBLE_DATA_TYPE → nome do Data Type no Bubble (ex: Acessorio)
"""

import csv
import os
import re
import time

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs

# ---------------------------------------------------------------------------
# Configurações do scraper
# ---------------------------------------------------------------------------
BASE_URL = "https://accessories.landrover.com/br/pt/"
SCRAPER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9",
}
DELAY = 0.5  # segundos entre requisições ao site

# ---------------------------------------------------------------------------
# Configurações do Bubble (lidas de variáveis de ambiente)
# ---------------------------------------------------------------------------
BUBBLE_APP_NAME  = os.environ.get("BUBBLE_APP_NAME", "")
BUBBLE_API_TOKEN = os.environ.get("BUBBLE_API_TOKEN", "")
BUBBLE_DATA_TYPE = os.environ.get("BUBBLE_DATA_TYPE", "")

BUBBLE_BASE_URL = (
    f"https://{BUBBLE_APP_NAME}.bubbleapps.io/api/1.1/obj/{BUBBLE_DATA_TYPE}"
    if BUBBLE_APP_NAME and BUBBLE_DATA_TYPE
    else ""
)

BUBBLE_HEADERS = {
    "Authorization": f"Bearer {BUBBLE_API_TOKEN}",
    "Content-Type": "application/json",
}


# ---------------------------------------------------------------------------
# Funções do scraper
# ---------------------------------------------------------------------------

def get_soup(url: str, session: requests.Session) -> BeautifulSoup | None:
    try:
        resp = session.get(url, headers=SCRAPER_HEADERS, timeout=15)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except requests.RequestException as e:
        print(f"  [ERRO scraper] {url} → {e}")
        return None


def get_brands(session: requests.Session) -> list[dict]:
    soup = get_soup(BASE_URL, session)
    if not soup:
        return []

    brands = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "brand=" not in href:
            continue
        parsed = urlparse(href)
        path_parts = [p for p in parsed.path.strip("/").split("/") if p]
        if len(path_parts) > 2:
            continue
        qs = parse_qs(parsed.query)
        brand_val = qs.get("brand", [None])[0]
        if brand_val and not any(b["brand"] == brand_val for b in brands):
            full_url = urljoin(BASE_URL, href)
            brands.append({"familia": brand_val, "brand": brand_val, "url": full_url})

    if not brands:
        for link in soup.select("nav a[href*='brand=']"):
            href = link["href"]
            qs = parse_qs(urlparse(href).query)
            brand_val = qs.get("brand", [None])[0]
            if brand_val and not any(b["brand"] == brand_val for b in brands):
                full_url = urljoin(BASE_URL, href)
                brands.append({"familia": brand_val, "brand": brand_val, "url": full_url})

    print(f"Famílias encontradas: {[b['brand'] for b in brands]}")
    return brands


def get_models(brand: dict, session: requests.Session) -> list[dict]:
    soup = get_soup(brand["url"], session)
    if not soup:
        return []

    models = []
    brand_val = brand["brand"]

    for a in soup.find_all("a", href=True):
        href = a["href"]
        parsed = urlparse(href)
        path_parts = [p for p in parsed.path.strip("/").split("/") if p]
        if len(path_parts) == 3 and path_parts[0] == "br" and path_parts[1] == "pt":
            modelo_slug = path_parts[2]
            qs = parse_qs(parsed.query)
            if qs.get("brand", [None])[0] == brand_val:
                full_url = urljoin("https://accessories.landrover.com", href)
                if not any(m["modelo"] == modelo_slug for m in models):
                    models.append({"modelo": modelo_slug, "url": full_url})

    print(f"  Modelos para {brand_val}: {[m['modelo'] for m in models]}")
    return models


def get_all_product_urls(model_url: str, session: requests.Session) -> list[str]:
    product_urls = []
    page = 1

    while True:
        url = f"{model_url}&page={page}" if "?" in model_url else f"{model_url}?page={page}"
        soup = get_soup(url, session)
        if not soup:
            break

        cards = soup.select("div.card a.card-contents, div.card > a[href]")
        if not cards:
            cards = soup.select("ul.grid li a[href*='/br/pt/']")

        new_urls = []
        for a in cards:
            href = a.get("href", "")
            if href and "/br/pt/" in href:
                full = urljoin("https://accessories.landrover.com", href.split("#")[0])
                if full not in product_urls and full not in new_urls:
                    new_urls.append(full)

        if not new_urls:
            break

        product_urls.extend(new_urls)
        print(f"    Página {page}: {len(new_urls)} produtos encontrados")

        next_btn = soup.select_one("a[rel='next'], a.pagination__next, li.next a")
        if not next_btn:
            break

        page += 1
        time.sleep(DELAY)

    return product_urls


def scrape_product(url: str, familia: str, modelo: str, session: requests.Session) -> dict | None:
    soup = get_soup(url, session)
    if not soup:
        return None

    codigo = None
    for strong in soup.find_all("strong"):
        if "Número do acessório" in strong.get_text() or "Numero do acessorio" in strong.get_text():
            parent = strong.parent
            full_text = parent.get_text(separator=" ", strip=True)
            match = re.search(r"Número do acessório\s*[:\u00a0]+\s*([A-Z0-9]+)", full_text, re.IGNORECASE)
            if match:
                codigo = match.group(1).strip()
                break

    if not codigo:
        code_el = soup.select_one("p.code, [class*='code']")
        if code_el:
            codigo = code_el.get_text(strip=True)

    imagem_url = None

    # Prioridade 1: img com data-testid contendo o código do acessório
    # Padrão: data-testid="VPLLE0052_feature_img"
    if codigo:
        testid = f"{codigo}_feature_img"
        img_el = soup.find("img", {"data-testid": testid})
        if img_el:
            src = img_el.get("src") or img_el.get("data-src")
            if src:
                imagem_url = src  # URL já é absoluta (assets.config.landrover.com)

    # Prioridade 2: qualquer img com data-testid terminando em _feature_img
    if not imagem_url:
        img_el = soup.find("img", {"data-testid": re.compile(r"_feature_img$")})
        if img_el:
            src = img_el.get("src") or img_el.get("data-src")
            if src:
                imagem_url = src

    # Prioridade 3: img dentro do lightbox (ol > li > img)
    if not imagem_url:
        lb_img = soup.select_one("section.lightbox ol li img")
        if lb_img:
            src = lb_img.get("src") or lb_img.get("data-src")
            if src:
                imagem_url = urljoin("https://accessories.landrover.com", src)

    # Prioridade 4: img em assets.config.landrover.com (imagens de acessório)
    if not imagem_url:
        for img in soup.find_all("img", src=True):
            src = img["src"]
            if "assets.config.landrover.com" in src and "/accessories/" in src:
                imagem_url = src
                break

    return {
        "familia": familia,
        "modelo": modelo,
        "codigo": codigo or "",
        "imagem_url": imagem_url or "",
    }


# ---------------------------------------------------------------------------
# Integração com Bubble
# ---------------------------------------------------------------------------

def send_to_bubble(row: dict) -> bool:
    if not BUBBLE_BASE_URL or not BUBBLE_API_TOKEN:
        return False

    payload = {
        "familia": row["familia"],
        "modelo": row["modelo"],
        "codigo": row["codigo"],
        "imagem_url": row["imagem_url"],
    }

    try:
        resp = requests.post(BUBBLE_BASE_URL, json=payload, headers=BUBBLE_HEADERS, timeout=15)
        if resp.status_code in (200, 201):
            data = resp.json()
            print(f"      → Bubble OK | id: {data.get('id', 'n/a')}")
            return True
        else:
            print(f"      → Bubble ERRO {resp.status_code}: {resp.text[:200]}")
            return False
    except requests.RequestException as e:
        print(f"      → Bubble EXCEÇÃO: {e}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    output_file = "landrover_acessorios.csv"
    fieldnames = ["familia", "modelo", "codigo", "imagem_url"]

    bubble_ativo = bool(BUBBLE_BASE_URL and BUBBLE_API_TOKEN)
    if bubble_ativo:
        print(f"🔗 Bubble ativo → {BUBBLE_BASE_URL}")
    else:
        print("⚠️  Bubble não configurado — apenas CSV será gerado.")

    with requests.Session() as session, open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        total_ok = 0
        total_bubble_ok = 0
        total_bubble_err = 0

        brands = get_brands(session)
        if not brands:
            print("Nenhuma família encontrada. Verifique a estrutura do site.")
            return

        for brand in brands:
            print(f"\n=== Família: {brand['brand']} ===")
            time.sleep(DELAY)

            models = get_models(brand, session)
            if not models:
                print("  Nenhum modelo encontrado.")
                continue

            for model in models:
                print(f"\n  Modelo: {model['modelo']}")
                time.sleep(DELAY)

                product_urls = get_all_product_urls(model["url"], session)
                print(f"  Total produtos no modelo: {len(product_urls)}")

                for i, prod_url in enumerate(product_urls, 1):
                    print(f"    [{i}/{len(product_urls)}] {prod_url}")
                    row = scrape_product(prod_url, brand["familia"], model["modelo"], session)

                    if row:
                        writer.writerow(row)
                        csvfile.flush()
                        total_ok += 1

                        if bubble_ativo:
                            ok = send_to_bubble(row)
                            if ok:
                                total_bubble_ok += 1
                            else:
                                total_bubble_err += 1

                    time.sleep(DELAY)

    print(f"\n✅ Concluído!")
    print(f"   CSV: {total_ok} produtos salvos em '{output_file}'")
    if bubble_ativo:
        print(f"   Bubble: {total_bubble_ok} enviados com sucesso, {total_bubble_err} erros")


if __name__ == "__main__":
    main()
