import csv
import os
import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs

# CONFIGURAÇÕES
BASE_URL = "https://accessories.landrover.com/br/pt/"
DELAY = 1.0  # Maior delay para evitar bloqueio de IP no GitHub

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,webp,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
}

def get_soup(url, session):
    try:
        r = session.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"      [!] Falha ao acessar {url}: {e}")
        return None

def get_brands(session):
    soup = get_soup(BASE_URL, session)
    brands = []
    if not soup: return brands
    for a in soup.find_all("a", href=True):
        if "brand=" in a["href"]:
            qs = parse_qs(urlparse(a["href"]).query)
            name = qs.get("brand", [None])[0]
            if name and name not in [b["brand"] for b in brands]:
                brands.append({"brand": name, "url": urljoin(BASE_URL, a["href"])})
    return brands

def get_models(brand, session):
    soup = get_soup(brand["url"], session)
    models = []
    if not soup: return models
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/br/pt/" in href and "brand=" in href:
            parts = [p for p in urlparse(href).path.strip("/").split("/") if p]
            if len(parts) == 3:
                modelo = parts[2]
                full_url = urljoin("https://accessories.landrover.com", href).split("?")[0] + f"?brand={brand['brand']}"
                if modelo not in [m["modelo"] for m in models]:
                    models.append({"modelo": modelo, "url": full_url})
    return models

def get_deep_categories(model_url, session):
    """Explora recursivamente menus de Exterior, Interior e Subcategorias"""
    soup = get_soup(model_url, session)
    categories = {model_url}
    if not soup: return categories

    # Procura links de categorias e subcategorias (ex: /exterior/exterior-styling/)
    model_path = urlparse(model_url).path
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if model_path in href and "brand=" in href:
            full = urljoin("https://accessories.landrover.com", href).split("#")[0]
            categories.add(full)
    return categories

def get_products(cat_url, session):
    products = set()
    page = 1
    while True:
        sep = "&" if "?" in cat_url else "?"
        url = f"{cat_url}{sep}page={page}"
        soup = get_soup(url, session)
        if not soup: break
        
        found = 0
        for a in soup.select("a[href*='/br/pt/']"):
            href = a.get("href", "")
            if "brand=" not in href and "#" not in href:
                full = urljoin("https://accessories.landrover.com", href).split("?")[0]
                if len(urlparse(full).path.strip("/").split("/")) >= 4:
                    products.add(full)
                    found += 1
        
        if found == 0 or page >= 5: break # Segurança contra loops
        page += 1
        time.sleep(DELAY)
    return products

def scrape_product(url, brand, model, session):
    soup = get_soup(url, session)
    if not soup: return None
    
    # Tenta extrair código da peça de várias formas
    text = soup.get_text()
    code_match = re.search(r"(?:peça:|acessório)\s*([A-Z0-9]{5,})", text, re.I)
    codigo = code_match.group(1) if code_match else None
    
    if not codigo: return None

    h1 = soup.find("h1")
    return {
        "familia": brand,
        "modelo": model,
        "codigo": codigo,
        "part_name": h1.get_text(strip=True) if h1 else "N/A",
        "url": url
    }

def main():
    session = requests.Session()
    output_file = "landrover_data.csv"
    processed = set()

    print(f"📝 Inicializando arquivo {output_file}...")
    
    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["familia", "modelo", "codigo", "part_name", "url"], delimiter=";")
        writer.writeheader()
        
        brands = get_brands(session)
        for b in brands:
            models = get_models(b, session)
            for m in models:
                print(f"🔎 Explorando Modelo: {m['modelo']}")
                # Entra em Exterior, Interior, etc.
                categories = get_deep_categories(m["url"], session)
                
                for cat in categories:
                    p_urls = get_products(cat, session)
                    for p_url in p_urls:
                        data = scrape_product(p_url, b["brand"], m["modelo"], session)
                        if data and data["codigo"] not in processed:
                            writer.writerow(data)
                            f.flush()
                            processed.add(data["codigo"])
                            print(f"   [+] {data['codigo']}")
                            time.sleep(DELAY)

    print(f"✅ Finalizado. {len(processed)} itens capturados.")

if __name__ == "__main__":
    main()
