import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By


USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

KEY_COLUMNS = [
    "brand",
    "asin",
    "product_name",
    "seller_name",
    "offer_condition",
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
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_run_utc": None, "last_status": None, "run_count": 0}


def build_driver(headless: bool = True) -> webdriver.Chrome:
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")

    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1800,2600")
    chrome_options.add_argument("--lang=fr-FR")
    chrome_options.add_argument(f"--user-agent={USER_AGENT}")

    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(60)
    return driver


def clean_text(text: Optional[str]) -> str:
    if not text:
        return ""
    text = text.replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def parse_price_to_float(text: Optional[str]) -> Optional[float]:
    if not text:
        return None

    s = clean_text(text).replace("€", "").replace(",", ".")
    s = re.sub(r"[^0-9. ]", "", s).replace(" ", "")

    if s.count(".") > 1:
        parts = s.split(".")
        s = "".join(parts[:-1]) + "." + parts[-1]

    try:
        return float(s)
    except ValueError:
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


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def current_scrape_column_name() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:00")


def extract_asin_from_url(url: str) -> Optional[str]:
    match = re.search(r"/dp/([A-Z0-9]{10})", url)
    if match:
        return match.group(1)
    return None


def is_new_offer(condition_text: str) -> bool:
    return "neuf" in condition_text.lower()


def is_excluded_seller(seller_name: str) -> bool:
    return clean_text(seller_name).lower() == "amazon seconde main"


def infer_shipping_cost_from_text(text: str) -> Optional[float]:
    txt = clean_text(text).lower()

    if not txt:
        return None

    if "livraison gratuite" in txt or "gratuite" in txt or "gratuit" in txt:
        return 0.0

    m = re.search(r"livraison[^0-9]*(\d+[.,]\d+)\s*€", txt, flags=re.IGNORECASE)
    if m:
        return parse_price_to_float(m.group(1))

    return None


def get_offer_page_url(asin: str) -> str:
    return f"https://www.amazon.fr/dp/{asin}/ref=olp-opf-redir?aod=1"


def scroll_page(driver: webdriver.Chrome) -> None:
    for y in [300, 900, 1500, 2200, 3000, 0]:
        driver.execute_script(f"window.scrollTo(0, {y});")
        time.sleep(1.2)


def save_debug_files(asin: str, html: str, screenshot_ok: bool, current_url: str, title: str) -> None:
    os.makedirs("datanomics/debug", exist_ok=True)

    html_path = f"datanomics/debug/{asin}_offers.html"
    meta_path = f"datanomics/debug/{asin}_meta.txt"

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    with open(meta_path, "w", encoding="utf-8") as f:
        f.write(f"ASIN: {asin}\n")
        f.write(f"Final URL: {current_url}\n")
        f.write(f"Title: {title}\n")
        f.write(f"Screenshot saved: {screenshot_ok}\n")


def extract_condition_from_row_text(text: str) -> str:
    low = text.lower()
    if "neuf" in low:
        return "Neuf"
    if "occasion" in low:
        return "Occasion"
    if "reconditionné" in low:
        return "Reconditionné"
    return ""


def extract_seller_from_row_text(text: str) -> str:
    patterns = [
        r"Vendu par\s+([^\n\r]+)",
        r"Vendeur\s+([^\n\r]+)",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            seller = clean_text(m.group(1))
            seller = re.sub(r"(Expédié par.*|Livraison.*|Détails.*)$", "", seller, flags=re.IGNORECASE)
            seller = clean_text(seller)
            if seller:
                return seller
    return ""


def extract_price_from_row_text(text: str) -> Optional[float]:
    matches = re.findall(r"(\d[\d\s.,]{0,12})\s*€", text)
    for raw in matches:
        p = parse_price_to_float(raw)
        if p is not None and p > 0:
            return p
    return None


def parse_offer_rows_from_html(html: str, brand: str, asin: str, product_name: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")

    selectors = [
        "div[id^='newAccordionRow_']",
        "div[id*='accordionRow']",
        "div.a-box-group",
        "div.a-section",
    ]

    row_nodes = []
    for sel in selectors:
        found = soup.select(sel)
        if found:
            print(f"[DEBUG] {asin} selector {sel} -> {len(found)} nodes")
            row_nodes = found
            break

    parsed_rows: List[Dict] = []

    for i, row in enumerate(row_nodes, start=1):
        row_text = clean_text(row.get_text("\n", strip=True))
        if not row_text:
            continue

        condition = extract_condition_from_row_text(row_text)
        seller = extract_seller_from_row_text(row_text)
        price_item = extract_price_from_row_text(row_text)
        shipping = infer_shipping_cost_from_text(row_text)

        print(
            f"[DEBUG] {asin} row #{i} | condition={condition} | "
            f"seller={seller} | price_item={price_item} | shipping={shipping}"
        )

        if not condition or not is_new_offer(condition):
            continue
        if not seller:
            continue
        if is_excluded_seller(seller):
            continue
        if price_item is None:
            continue

        total = price_item + (shipping or 0.0)

        parsed_rows.append(
            {
                "brand": brand,
                "asin": asin,
                "product_name": product_name,
                "seller_name": seller,
                "offer_condition": condition,
                "price_total_eur": round(total, 2),
            }
        )

    deduped = []
    seen = set()
    for row in parsed_rows:
        key = (
            row["brand"],
            row["asin"],
            row["product_name"],
            row["seller_name"].strip().lower(),
            row["offer_condition"].strip().lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    print(f"[DEBUG] {asin} valid rows: {len(deduped)}")
    return deduped


def scrape_product(driver: webdriver.Chrome, brand: str, product: Dict) -> List[Dict]:
    product_url = product["product_url"]
    asin = product.get("asin") or extract_asin_from_url(product_url)
    product_name = product.get("product_name", "")

    if not asin:
        print(f"[DEBUG] Missing ASIN for {product_url}")
        return []

    offer_url = get_offer_page_url(asin)
    print(f"[DEBUG] Opening {offer_url}")

    driver.get(offer_url)
    time.sleep(7)
    scroll_page(driver)

    html = driver.page_source
    title = driver.title
    current_url = driver.current_url

    screenshot_path = f"datanomics/debug/{asin}_offers.png"
    os.makedirs("datanomics/debug", exist_ok=True)
    screenshot_ok = driver.save_screenshot(screenshot_path)

    save_debug_files(
        asin=asin,
        html=html,
        screenshot_ok=screenshot_ok,
        current_url=current_url,
        title=title,
    )

    lower = html.lower()
    if "captcha" in lower:
        print(f"[DEBUG] CAPTCHA detected for {asin}")
        return []
    if "robot" in lower:
        print(f"[DEBUG] Robot check detected for {asin}")
        return []

    return parse_offer_rows_from_html(
        html=html,
        brand=brand,
        asin=asin,
        product_name=product_name,
    )


def update_tracker_csv(csv_path: str, scraped_rows: List[Dict], scrape_col: str) -> None:
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
        df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
    else:
        df = pd.DataFrame(columns=KEY_COLUMNS)

    for col in KEY_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    if scrape_col not in df.columns:
        df[scrape_col] = ""

    allowed = set(KEY_COLUMNS + [c for c in df.columns if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:00$", str(c))])
    df = df[[c for c in df.columns if c in allowed]]

    if scrape_col not in df.columns:
        df[scrape_col] = ""

    for row in scraped_rows:
        key_values = {k: str(row.get(k, "")) for k in KEY_COLUMNS}
        price_value = row.get("price_total_eur", "")

        mask = pd.Series([True] * len(df))
        for key_col in KEY_COLUMNS:
            mask = mask & (df[key_col].fillna("").astype(str) == key_values[key_col])

        if mask.any():
            idx = df[mask].index[0]
            df.at[idx, scrape_col] = price_value
        else:
            new_row = {col: "" for col in df.columns}
            for key_col in KEY_COLUMNS:
                new_row[key_col] = key_values[key_col]
            new_row[scrape_col] = price_value
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

    time_cols = sorted([c for c in df.columns if c not in KEY_COLUMNS])
    df = df[KEY_COLUMNS + time_cols]
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to config JSON")
    parser.add_argument("--headful", action="store_true")
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
            rows = scrape_product(driver=driver, brand=brand, product=product)
            all_rows.extend(rows)
            time.sleep(2)

        update_tracker_csv(output_csv, all_rows, scrape_col)

        new_state = {
            "last_run_utc": now_utc_str(),
            "last_status": f"ok_rows={len(all_rows)}",
            "run_count": int(state.get("run_count", 0)) + 1,
        }
        save_json(state_file, new_state)

        print(f"[OK] {len(all_rows)} rows written")

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
