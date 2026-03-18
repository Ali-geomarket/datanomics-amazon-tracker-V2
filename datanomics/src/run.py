import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By


USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

TRACKER_KEY_COLUMNS = [
    "brand",
    "asin",
    "product_name",
    "product_url",
    "offers_url",
    "seller_name",
    "offer_condition",
]

BASE_COLUMNS = TRACKER_KEY_COLUMNS + [
    "last_seen_utc",
    "last_scrape_status",
]


def load_json(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: Dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_state(path: str) -> Dict:
    if os.path.exists(path):
        return load_json(path)
    return {
        "last_run_utc": None,
        "last_status": None,
        "run_count": 0,
    }


def build_driver(headless: bool = True) -> webdriver.Chrome:
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")

    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1600,2600")
    chrome_options.add_argument("--lang=fr-FR")
    chrome_options.add_argument(f"--user-agent={USER_AGENT}")

    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(60)
    return driver


def clean_text(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def parse_price_to_float(text: Optional[str]) -> Optional[float]:
    if not text:
        return None

    s = text.replace("\xa0", " ").replace("€", "").strip()
    s = s.replace(",", ".")
    s = re.sub(r"[^0-9. ]", "", s)
    s = s.replace(" ", "")

    if s.count(".") > 1:
        parts = s.split(".")
        s = "".join(parts[:-1]) + "." + parts[-1]

    try:
        return float(s)
    except ValueError:
        return None


def extract_asin_from_url(url: str) -> Optional[str]:
    match = re.search(r"/dp/([A-Z0-9]{10})", url)
    if match:
        return match.group(1)
    return None


def accept_or_reject_cookies(driver: webdriver.Chrome) -> None:
    selectors = [
        (By.ID, "sp-cc-rejectall-link"),
        (By.ID, "sp-cc-accept"),
    ]
    for by, value in selectors:
        try:
            btn = driver.find_element(by, value)
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(1)
            return
        except Exception:
            continue


def infer_shipping_cost(shipping_text: str) -> Optional[float]:
    txt = shipping_text.lower()
    if not txt:
        return None

    if "gratuite" in txt or "gratuit" in txt or "free" in txt:
        return 0.0

    m = re.search(r"(\d+[.,]\d+)\s*€", shipping_text)
    if m:
        return parse_price_to_float(m.group(1))

    return None


def is_new_offer(condition_text: str) -> bool:
    return "neuf" in condition_text.lower()


def is_excluded_seller(seller_name: str) -> bool:
    txt = seller_name.strip().lower()
    return txt == "amazon seconde main"


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def current_scrape_column_name() -> str:
    # colonne horaire arrondie à l'heure pour le tracker
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:00")


def scroll_page(driver: webdriver.Chrome) -> None:
    positions = [0.2, 0.45, 0.7, 1.0]
    for p in positions:
        driver.execute_script(
            "window.scrollTo(0, document.body.scrollHeight * arguments[0]);", p
        )
        time.sleep(1.5)
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(1)


def find_offer_containers(driver: webdriver.Chrome):
    """
    On cherche des conteneurs d'offres avec plusieurs stratégies.
    L'idée est d'éviter les sélecteurs trop fragiles.
    """
    candidates = []

    xpaths = [
        # blocs Amazon AOD typiques
        "//div[starts-with(@id, 'aod-offer-')]",
        # blocs contenant explicitement Vendeur et Expéditeur
        "//div[contains(., 'Vendeur') and contains(., 'Expéditeur')]",
        # fallback générique si la structure change
        "//div[contains(., 'Vendeur') and contains(., 'Neuf')]",
    ]

    for xp in xpaths:
        try:
            elems = driver.find_elements(By.XPATH, xp)
            if elems:
                print(f"[DEBUG] XPath '{xp}' -> {len(elems)} blocs")
                candidates = elems
                break
        except Exception:
            continue

    # dédoublonnage simple par texte
    unique = []
    seen = set()
    for elem in candidates:
        try:
            txt = clean_text(elem.text)
            if not txt:
                continue
            key = txt[:400]
            if key in seen:
                continue
            seen.add(key)
            unique.append(elem)
        except Exception:
            continue

    print(f"[DEBUG] Blocs d'offres retenus après dédoublonnage : {len(unique)}")
    return unique


def extract_price_from_text(block_text: str) -> Optional[float]:
    # prix principal sur une ligne de type 26300€ ou 263,00 €
    patterns = [
        r"(\d[\d\s.,]{0,12})\s*€",
    ]
    for pattern in patterns:
        matches = re.findall(pattern, block_text)
        if matches:
            for m in matches:
                value = parse_price_to_float(m)
                if value is not None and value > 0:
                    return value
    return None


def extract_condition_from_text(block_text: str) -> str:
    text_low = block_text.lower()
    if "neuf" in text_low:
        return "Neuf"
    if "occasion" in text_low:
        return "Occasion"
    if "reconditionné" in text_low:
        return "Reconditionné"
    return ""


def extract_seller_from_text(block_text: str) -> str:
    """
    On récupère d'abord après 'Vendeur', sinon après 'Expéditeur',
    sinon fallback via les liens internes.
    """
    patterns = [
        r"Vendeur\s+([^\n\r]+)",
        r"Expéditeur\s+([^\n\r]+)",
    ]
    for pattern in patterns:
        m = re.search(pattern, block_text, flags=re.IGNORECASE)
        if m:
            seller = clean_text(m.group(1))
            # nettoyage de texte parasite
            seller = re.sub(r"(Évaluation.*|[0-9]+ ?évaluations.*|[0-9]+ ?% positif.*)$", "", seller, flags=re.IGNORECASE)
            seller = clean_text(seller)
            if seller:
                return seller
    return ""


def extract_shipping_from_text(block_text: str) -> Tuple[Optional[float], str]:
    shipping_lines = []
    for line in block_text.split("\n"):
        line_clean = clean_text(line)
        if not line_clean:
            continue
        low = line_clean.lower()
        if "livraison" in low or "gratuite" in low or "gratuit" in low:
            shipping_lines.append(line_clean)

    shipping_text = " | ".join(shipping_lines)
    shipping_cost = infer_shipping_cost(shipping_text)
    return shipping_cost, shipping_text


def parse_offer_block_text(block_text: str) -> Dict:
    condition_text = extract_condition_from_text(block_text)
    seller_name = extract_seller_from_text(block_text)
    price_item = extract_price_from_text(block_text)
    shipping_cost, shipping_text = extract_shipping_from_text(block_text)

    return {
        "offer_condition": condition_text,
        "seller_name": seller_name,
        "price_item_eur": price_item,
        "shipping_eur": shipping_cost,
        "shipping_text": shipping_text,
    }


def scrape_offer_listing(
    driver: webdriver.Chrome,
    brand: str,
    asin: str,
    product_name: str,
    product_url: str,
) -> List[Dict]:
    offers_url = f"https://www.amazon.fr/gp/offer-listing/{asin}"
    rows: List[Dict] = []

    driver.get(offers_url)
    time.sleep(6)
    scroll_page(driver)

    print(f"[DEBUG] URL ouverte : {offers_url}")
    print(f"[DEBUG] URL finale : {driver.current_url}")
    print(f"[DEBUG] Titre page : {driver.title}")

    page_source_lower = driver.page_source.lower()
    if "captcha" in page_source_lower:
        print("[DEBUG] CAPTCHA détecté")
    if "robot" in page_source_lower:
        print("[DEBUG] Contrôle robot détecté")

    containers = find_offer_containers(driver)

    for i, container in enumerate(containers, start=1):
        try:
            block_text = clean_text(container.text)
            print(f"[DEBUG] Bloc #{i} aperçu : {block_text[:350]}")

            parsed = parse_offer_block_text(block_text)

            condition_text = parsed["offer_condition"]
            seller_name = parsed["seller_name"]
            price_item = parsed["price_item_eur"]
            shipping_cost = parsed["shipping_eur"]

            print(
                f"[DEBUG] Bloc #{i} -> condition={condition_text} | "
                f"seller={seller_name} | price={price_item} | shipping={shipping_cost}"
            )

            if not condition_text or not is_new_offer(condition_text):
                continue

            if not seller_name:
                continue

            if is_excluded_seller(seller_name):
                continue

            if price_item is None:
                continue

            price_total = price_item + (shipping_cost or 0.0)

            rows.append(
                {
                    "brand": brand,
                    "asin": asin,
                    "product_name": product_name,
                    "product_url": product_url,
                    "offers_url": offers_url,
                    "seller_name": seller_name,
                    "offer_condition": condition_text,
                    "price_item_eur": price_item,
                    "shipping_eur": shipping_cost if shipping_cost is not None else "",
                    "price_total_eur": price_total,
                    "scrape_status": "ok",
                }
            )

        except Exception as e:
            print(f"[DEBUG] Erreur parsing bloc #{i}: {e}")
            continue

    # dédoublonnage final
    deduped = []
    seen = set()
    for row in rows:
        key = (
            row["asin"],
            row["seller_name"].strip().lower(),
            row["offer_condition"].strip().lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    print(f"[DEBUG] Offres neuves valides extraites : {len(deduped)}")
    return deduped


def build_fallback_row(
    brand: str,
    asin: str,
    product_name: str,
    product_url: str,
    status: str,
) -> Dict:
    return {
        "brand": brand,
        "asin": asin,
        "product_name": product_name,
        "product_url": product_url,
        "offers_url": f"https://www.amazon.fr/gp/offer-listing/{asin}" if asin else "",
        "seller_name": "__NO_SELLER_FOUND__",
        "offer_condition": "",
        "price_item_eur": "",
        "shipping_eur": "",
        "price_total_eur": "",
        "scrape_status": status,
    }


def update_tracker_csv(csv_path: str, scraped_rows: List[Dict], scrape_col: str) -> None:
    """
    1 ligne par couple produit x vendeur x état
    1 nouvelle colonne par heure de scraping
    """
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
        df_existing = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
    else:
        df_existing = pd.DataFrame(columns=BASE_COLUMNS)

    # garantir colonnes de base
    for col in BASE_COLUMNS:
        if col not in df_existing.columns:
            df_existing[col] = ""

    if scrape_col not in df_existing.columns:
        df_existing[scrape_col] = ""

    if not scraped_rows:
        df_existing.to_csv(csv_path, index=False, encoding="utf-8-sig")
        return

    for row in scraped_rows:
        key_values = {k: str(row.get(k, "")) for k in TRACKER_KEY_COLUMNS}
        price_value = row.get("price_total_eur", "")
        scrape_status = row.get("scrape_status", "")
        last_seen_utc = now_utc_str()

        mask = pd.Series([True] * len(df_existing))
        for key_col in TRACKER_KEY_COLUMNS:
            mask = mask & (df_existing[key_col].fillna("").astype(str) == key_values[key_col])

        if mask.any():
            idx = df_existing[mask].index[0]
            df_existing.at[idx, scrape_col] = price_value
            df_existing.at[idx, "last_seen_utc"] = last_seen_utc
            df_existing.at[idx, "last_scrape_status"] = scrape_status
        else:
            new_row = {col: "" for col in df_existing.columns}
            for key_col in TRACKER_KEY_COLUMNS:
                new_row[key_col] = key_values[key_col]
            new_row["last_seen_utc"] = last_seen_utc
            new_row["last_scrape_status"] = scrape_status
            new_row[scrape_col] = price_value
            df_existing = pd.concat([df_existing, pd.DataFrame([new_row])], ignore_index=True)

    # ordre des colonnes : base d'abord puis colonnes horaires triées
    fixed_cols = [c for c in BASE_COLUMNS if c in df_existing.columns]
    time_cols = [
        c for c in df_existing.columns
        if c not in fixed_cols
    ]
    time_cols_sorted = sorted(
        time_cols,
        key=lambda x: (0, x) if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:00$", x) else (1, x)
    )

    df_existing = df_existing[fixed_cols + time_cols_sorted]
    df_existing.to_csv(csv_path, index=False, encoding="utf-8-sig")


def scrape_product(
    driver: webdriver.Chrome,
    brand: str,
    product: Dict,
) -> List[Dict]:
    product_url = product["product_url"]
    asin = product.get("asin") or extract_asin_from_url(product_url)
    product_name = product.get("product_name", "")

    if not asin:
        return [
            build_fallback_row(
                brand=brand,
                asin="",
                product_name=product_name,
                product_url=product_url,
                status="error_asin_missing",
            )
        ]

    try:
        rows = scrape_offer_listing(
            driver=driver,
            brand=brand,
            asin=asin,
            product_name=product_name,
            product_url=product_url,
        )
        if rows:
            return rows

        return [
            build_fallback_row(
                brand=brand,
                asin=asin,
                product_name=product_name,
                product_url=product_url,
                status="no_new_offers_found",
            )
        ]

    except Exception as e:
        print(f"[DEBUG] Erreur scrape produit {asin}: {e}")
        return [
            build_fallback_row(
                brand=brand,
                asin=asin,
                product_name=product_name,
                product_url=product_url,
                status=f"error: {str(e)}",
            )
        ]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to config JSON")
    parser.add_argument("--headful", action="store_true", help="Run with visible browser")
    args = parser.parse_args()

    config = load_json(args.config)
    brand = config["brand"]
    output_csv = config["output_csv"]
    state_file = config["state_file"]
    products = config["products"]

    state = load_state(state_file)
    scrape_col = current_scrape_column_name()

    driver = build_driver(headless=not args.headful)

    try:
        driver.get("https://www.amazon.fr")
        time.sleep(3)
        accept_or_reject_cookies(driver)

        all_rows: List[Dict] = []

        for product in products:
            rows = scrape_product(
                driver=driver,
                brand=brand,
                product=product,
            )
            all_rows.extend(rows)
            time.sleep(2)

        update_tracker_csv(output_csv, all_rows, scrape_col)

        new_state = {
            "last_run_utc": now_utc_str(),
            "last_status": "ok",
            "run_count": int(state.get("run_count", 0)) + 1,
        }
        save_json(state_file, new_state)

        print(f"[OK] {len(all_rows)} lignes traitées dans {output_csv}")

    except Exception as e:
        error_state = {
            "last_run_utc": now_utc_str(),
            "last_status": f"error: {str(e)}",
            "run_count": int(state.get("run_count", 0)) + 1,
        }
        save_json(state_file, error_state)
        print(f"[ERROR] {e}", file=sys.stderr)
        raise

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
