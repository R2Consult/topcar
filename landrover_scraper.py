import csv
import json
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
DELAY = 0.5 # Aumentado levemente para estabilidade

BUBBLE_APP_NAME  = os.environ.get("BUBBLE_APP_NAME", "")
BUBBLE_API_TOKEN = os.environ.get("BUBBLE_API_TOKEN", "")
BUBBLE_DATA_TYPE = os.environ.get("BUBBLE_DATA_TYPE", "")

BUBBLE_URL = (
    f"https://{BUBBLE_APP_NAME}.bubbleapps.io/api/1.1/obj/{BUBBLE_DATA_TYPE}"
    if BUBBLE_APP_NAME and BUBBLE_DATA_TYPE else ""
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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
    if not value: return ""
    return str(value).replace("\n", " ").replace("\r", " ").replace("\t", " ").strip()

def clean_url(value):
    v = clean(value)
    if v and v.startswith("http"):
        return v
    return ""  

def get_soup(url, session):
    try:
        r = session.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"      Erro ao acessar {url}: {e}")
        return None

# ---------------------------------------------------------------------------
# SCRAPER
# ---------------------------------------------------------------------------

def get_brands(session):
    soup = get_soup(BASE_URL, session)
    brands = []
    if not soup: return brands

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "brand=" in href:
            parsed = urlparse(href)
            qs = parse_qs(parsed.query)
            brand_name = qs.get("brand", [None])[0]
            if brand_name and brand_name not in [b["brand"] for b in brands]:
                brands.append({
                    "familia": brand_name,
                    "brand": brand_name,
                    "url": urljoin(BASE_URL, href)
                })
    return brands

def get_models(brand, session):
    soup = get_soup(brand["url"], session)
    models = []
    if not soup: return models

    brand_val = brand["brand"]
    # Busca links de modelos específicos
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/br/pt/" in href and "brand=" in href:
            parsed = urlparse(href)
            parts = [p for p in parsed.path.strip("/").split("/") if p]
            # Modelos geralmente estão no 3º nível: /br/pt/modelo
            if len(parts) == 3:
                modelo = parts[2]
                full_url = urljoin("https://accessories.landrover.com", href).split("#")[0].split("?")[0]
                # Adiciona query param da marca para manter contexto do site
                full_url += f"?brand={brand_val}"
                
                if modelo not in [m["modelo"] for m in models]:
                    models.append({
                        "modelo": modelo,
                        "url": full_url
                    })
    return models

def get_category_links(model_url, session):
    """Explora a página do modelo em busca de seções (Exterior, Interior, etc)"""
    soup = get_soup(model_url, session)
    cat_links = {model_url} # Começa com a própria URL do modelo
    if not soup: return cat_links

    # Busca links que pareçam categorias ou subcategorias dentro do contexto do modelo
    # Ex: /br/pt/range-rover/exterior/
    model_path = urlparse(model_url).path
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if model_path in href and "brand=" in href:
            full = urljoin("https://accessories.landrover.com", href).split("#")[0]
            cat_links.add(full)
            
    return list(cat_links)

def get_products_from_page(page_url, session):
    """Extrai links de produtos de uma página específica (categoria ou listagem)"""
    urls = set()
    page = 1
    
    while True:
        sep = "&" if "?" in page_url else "?"
        url = f"{page_url}{sep}page={page}"
        soup = get_soup(url, session)
        if not soup: break

        found_in_page = 0
        # O seletor foca em links que levam à página final do produto (part)
        for a in soup.select("a[href*='/br/pt/']"):
            href = a.get("href", "")
            # Produtos geralmente têm URLs mais profundas ou palavras chave
            # Filtramos para evitar links de navegação comuns
            if any(x in href for x in ["#", "brand=", "javascript:"]): continue
            
            full = urljoin("https://accessories.landrover.com", href).split("?")[0]
            
            # Verifica se o link parece ser de um produto (geralmente termina com o nome do acessório)
            # e evita duplicar categorias
            parts = [p for p in urlparse(full).path.strip("/").split("/") if p]
            if len(parts) >= 4: # /br/pt/modelo/categoria/produto
                if full not in urls:
                    urls.add(full)
                    found_in_page += 1

        if found_in_page == 0 or page > 10: # Limite de segurança de 10 páginas por subseção
            break
        
        page += 1
        time.sleep(DELAY)
        
    return urls

def scrape_product(url, familia, modelo, session):
    soup = get_soup(url, session)
    if not soup: return None

    # Código do acessório (Part Number)
    codigo = ""
    for strong in soup.find_all(["strong", "span", "p"]):
        txt = strong.get_text()
        if "Número da peça" in txt or "acessório" in txt.lower():
            match = re.search(r"([A-Z0-9]{5,})", strong.parent.get_text())
            if match:
                codigo = match.group(1)
                break
    
    if not codigo:
        # Tenta pegar de elementos de dados do site
        code_el = soup.select_one(".part-code, .product-code")
        if code_el: codigo = code_el.get_text(strip=True)

    # Imagem do acessório
    imagem_url = ""
    img_tags = [
        soup.find("img", {"data-testid": re.compile(r"_feature_img$")}),
        soup.select_one("section.part-summary img"),
        soup.select_one(".product-image img")
    ]
    for tag in img_tags:
        if tag:
            imagem_url = tag.get("src") or tag.get("data-src")
            if imagem_url: break

    if not imagem_url:
        for img in soup.find_all("img", src=True):
            if "assets.config.landrover.com/accessories/" in img["src"]:
                imagem_url = img["src"]
                break

    # Nome e Descrição
    part_name = ""
    h1 = soup.find("h1")
    if h1: part_name = h1.get_text(strip=True)

    descricao = ""
    desc_el = soup.select_one("section.part-summary .text-container p") or soup.select_one(".description p")
    if desc_el:
        descricao = desc_el.get_text(strip=True)
    else:
        # Fallback: pega o primeiro parágrafo longo
        for p in soup.find_all("p"):
            if len(p.get_text()) > 50:
                descricao = p.get_text(strip=True)
                break

    return {
        "familia": clean(familia),
        "modelo": clean(modelo),
        "codigo": clean(codigo),
        "part_name": clean(part_name),
        "descricao": clean(descricao),
        "imagem_url": clean_url(imagem_url),
    }

# ---------------------------------------------------------------------------
# BUBBLE INTEGRATION (Mantida a sua lógica original)
# ---------------------------------------------------------------------------

def send_to_bubble(row):
    if not BUBBLE_URL: return False
    for attempt in range(2):
        try:
            r = requests.post(BUBBLE_URL, json=row, headers=BUBBLE_HEADERS, timeout=10)
            if r.status_code in [200, 201]: return True
        except: pass
        time.sleep(1)
    return False

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    session = requests.Session()
    bubble_ativo = bool(BUBBLE_URL and BUBBLE_API_TOKEN)
    processed_codes = set() # Evita duplicados na mesma execução

    print(f"🚀 Iniciando Scraper...")

    with open("landrover_completo.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["familia", "modelo", "codigo", "part_name", "descricao", "imagem_url"], delimiter=";")
        writer.writeheader()

        for brand in get_brands(session):
            print(f"\n--- MARCA: {brand['familia']} ---")
            
            for model in get_models(brand, session):
                print(f"  > Modelo: {model['modelo']}")
                
                # 1. Mapeia todas as categorias/subseções do modelo
                subsections = get_category_links(model["url"], session)
                print(f"    Encontradas {len(subsections)} subseções para explorar.")

                for sub_url in subsections:
                    # 2. Coleta produtos da subseção
                    product_urls = get_products_from_page(sub_url, session)
                    
                    for p_url in product_urls:
                        # 3. Scrape individual
                        data = scrape_product(p_url, brand["familia"], model["modelo"], session)
                        
                        if data and data["codigo"] and data["codigo"] not in processed_codes:
                            writer.writerow(data)
                            f.flush()
                            processed_codes.add(data["codigo"])
                            
                            status = "CSV"
                            if bubble_ativo:
                                ok = send_to_bubble(data)
                                status = "BUBBLE OK" if ok else "BUBBLE ERRO"
                            
                            print(f"      [{status}] {data['codigo']} - {data['part_name'][:30]}...")
                            time.sleep(DELAY)

    print(f"\n✅ Concluído! Total de itens únicos: {len(processed_codes)}")

if __name__ == "__main__":
    main()
