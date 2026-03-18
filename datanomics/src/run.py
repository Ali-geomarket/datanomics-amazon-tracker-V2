import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

CSV_COLUMNS = [
    "scrape_timestamp_utc",
    "batch_id",
    "brand",
    "asin",
    "product_name",
    "product_url",
    "offers_url",
    "seller_name",
    "offer_condition",
    "price_item_eur",
    "shipping_eur",
    "price_total_eur",
    "is_free_shipping",
    "is_main_offer",
    "offer_rank",
    "scrape_status",
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
        "last_batch_id": -1,
        "last_run_utc": None,
        "last_status": None,
        "run_count": 0,
    }


def append_rows_to_csv(csv_path: str, rows: List[Dict]) -> None:
    if not rows:
        return

    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    file_exists = os.path.exists(csv_path)

    with open(csv_path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


def build_driver(headless: bool = True) -> webdriver.Chrome:
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")

    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1600,2200")
    chrome_options.add_argument("--lang=fr-FR")
    chrome_options.add_argument(f"--user-agent={USER_AGENT}")

    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(60)
    return driver


def accept_or_reject_cookies(driver: webdriver.Chrome) -> None:
    possible_selectors = [
        (By.ID, "sp-cc-rejectall-link"),
        (By.ID, "sp-cc-accept"),
        (By.XPATH, "//input[@name='accept']"),
        (By.XPATH, "//span[contains(text(), 'Refuser') or contains(text(), 'Accepter')]"),
    ]

    for by, value in possible_selectors:
        try:
            btn = WebDriverWait(driver, 4).until(
                EC.element_to_be_clickable((by, value))
            )
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(1)
            return
        except Exception:
            continue


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


def safe_find_text(parent, by: By, value: str) -> str:
    try:
        return clean_text(parent.find_element(by, value).text)
    except Exception:
        return ""


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
    txt = condition_text.lower()
    return "neuf" in txt


def is_excluded_seller(seller_name: str) -> bool:
    txt = seller_name.strip().lower()
    return txt == "amazon seconde main"


def get_product_title(driver: webdriver.Chrome) -> str:
    selectors = [
        (By.ID, "productTitle"),
        (By.CSS_SELECTOR, "h1 span"),
        (By.CSS_SELECTOR, "span#title"),
    ]
    for by, value in selectors:
        try:
            text = clean_text(driver.find_element(by, value).text)
            if text:
                return text
        except Exception:
            continue
    return ""


def scrape_main_offer(
    driver: webdriver.Chrome,
    brand: str,
    asin: str,
    product_name: str,
    product_url: str,
    scrape_ts: str,
    batch_id: int,
) -> List[Dict]:
    rows = []

    driver.get(product_url)
    time.sleep(3)

    page_title = get_product_title(driver)
    if page_title:
        product_name = page_title

    price_item = None
    shipping_cost = None
    seller_name = ""
    condition_text = "Neuf"

    price_candidates = [
        (By.CSS_SELECTOR, "span.a-price span.a-offscreen"),
        (By.CSS_SELECTOR, "#corePrice_feature_div span.a-offscreen"),
        (By.CSS_SELECTOR, "#tp_price_block_total_price_ww span.a-offscreen"),
    ]
    for by, value in price_candidates:
        try:
            elem_text = clean_text(driver.find_element(by, value).text)
            price_item = parse_price_to_float(elem_text)
            if price_item is not None:
                break
        except Exception:
            continue

    shipping_candidates = [
        (By.CSS_SELECTOR, "[data-csa-c-delivery-price]"),
        (By.ID, "deliveryBlockMessage"),
        (By.ID, "mir-layout-DELIVERY_BLOCK-slot-PRIMARY_DELIVERY_MESSAGE_LARGE"),
    ]
    shipping_text = ""
    for by, value in shipping_candidates:
        try:
            elem = driver.find_element(by, value)
            shipping_text = clean_text(
                elem.get_attribute("data-csa-c-delivery-price") or elem.text
            )
            if shipping_text:
                shipping_cost = infer_shipping_cost(shipping_text)
                break
        except Exception:
            continue

    seller_candidates = [
        (By.ID, "sellerProfileTriggerId"),
        (By.CSS_SELECTOR, "#merchantInfo a"),
        (By.CSS_SELECTOR, "a#sellerProfileTriggerId"),
    ]
    for by, value in seller_candidates:
        try:
            seller_name = clean_text(driver.find_element(by, value).text)
            if seller_name:
                break
        except Exception:
            continue

    if not seller_name:
        merchant_text = safe_find_text(driver, By.ID, "merchantInfo")
        if merchant_text:
            seller_name = merchant_text

    if price_item is not None and seller_name and not is_excluded_seller(seller_name):
        total_price = price_item + (shipping_cost or 0.0)
        rows.append(
            {
                "scrape_timestamp_utc": scrape_ts,
                "batch_id": batch_id,
                "brand": brand,
                "asin": asin,
                "product_name": product_name,
                "product_url": product_url,
                "offers_url": f"https://www.amazon.fr/gp/offer-listing/{asin}",
                "seller_name": seller_name,
                "offer_condition": condition_text,
                "price_item_eur": price_item,
                "shipping_eur": shipping_cost if shipping_cost is not None else "",
                "price_total_eur": total_price,
                "is_free_shipping": 1 if shipping_cost == 0 else 0,
                "is_main_offer": 1,
                "offer_rank": 0,
                "scrape_status": "ok",
            }
        )

    return rows


def extract_offer_blocks(driver: webdriver.Chrome):
    time.sleep(3)

    # petit scroll pour forcer le rendu complet
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.35);")
    time.sleep(2)
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.65);")
    time.sleep(2)
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(1)

    selectors = [
        "div[id^='aod-offer-']",
        "div.aod-information-block",
        "div.a-section.a-spacing-none.a-padding-base",
        "div[data-cy='offer']",
        "div.olpOffer",
    ]

    for css in selectors:
        blocks = driver.find_elements(By.CSS_SELECTOR, css)
        if blocks:
            print(f"[DEBUG] selector '{css}' -> {len(blocks)} blocks found")
            return blocks

    print("[DEBUG] No offer blocks found with known selectors")
    return []


def extract_offer_condition(block) -> str:
    candidates = [
        (By.CSS_SELECTOR, "div#aod-offer-heading"),
        (By.CSS_SELECTOR, "div[id*='aod-offer-heading']"),
        (By.CSS_SELECTOR, "h5"),
        (By.CSS_SELECTOR, "span"),
    ]

    for by, value in candidates:
        try:
            elems = block.find_elements(by, value)
            for elem in elems:
                text = clean_text(elem.text)
                if text and ("neuf" in text.lower() or "occasion" in text.lower() or "reconditionné" in text.lower()):
                    return text
        except Exception:
            continue
    return ""

def extract_offer_price(block) -> Optional[float]:
    candidates = [
        (By.CSS_SELECTOR, "span.a-price span.a-offscreen"),
        (By.CSS_SELECTOR, ".a-price .a-offscreen"),
        (By.CSS_SELECTOR, "span.a-price"),
    ]
    for by, value in candidates:
        try:
            text = clean_text(block.find_element(by, value).text)
            value_num = parse_price_to_float(text)
            if value_num is not None:
                return value_num
        except Exception:
            continue
    return None


def extract_offer_shipping(block) -> (Optional[float], str):
    texts = []

    selectors = [
        (By.CSS_SELECTOR, "[data-csa-c-delivery-price]"),
        (By.CSS_SELECTOR, "div[id*='DELIVERY_BLOCK']"),
        (By.CSS_SELECTOR, "span[data-csa-c-content-id]"),
        (By.CSS_SELECTOR, "div.a-row.a-size-base.a-color-secondary"),
    ]

    for by, value in selectors:
        try:
            elems = block.find_elements(by, value)
            for elem in elems:
                txt = clean_text(
                    elem.get_attribute("data-csa-c-delivery-price") or elem.text
                )
                if txt:
                    texts.append(txt)
        except Exception:
            continue

    joined = " | ".join(texts)
    return infer_shipping_cost(joined), joined


def extract_offer_seller(block) -> str:
    candidates = [
        (By.CSS_SELECTOR, "#aod-offer-soldBy a"),
        (By.CSS_SELECTOR, "#aod-offer-soldBy span.a-size-small"),
        (By.CSS_SELECTOR, "a[role='link']"),
    ]
    for by, value in candidates:
        try:
            text = clean_text(block.find_element(by, value).text)
            if text:
                return text
        except Exception:
            continue
    return ""


def scrape_offer_listing(
    driver: webdriver.Chrome,
    brand: str,
    asin: str,
    product_name: str,
    product_url: str,
    scrape_ts: str,
    batch_id: int,
) -> List[Dict]:
    offers_url = f"https://www.amazon.fr/gp/offer-listing/{asin}"
    rows = []

    driver.get(offers_url)
    time.sleep(6)

    print(f"[DEBUG] Opened offers page: {offers_url}")
    print(f"[DEBUG] Current URL: {driver.current_url}")
    print(f"[DEBUG] Page title: {driver.title}")

    page_source_lower = driver.page_source.lower()

    if "captcha" in page_source_lower:
        print("[DEBUG] CAPTCHA detected on offers page")
    if "robot" in page_source_lower:
        print("[DEBUG] Robot check detected on offers page")
    if "503" in driver.title:
        print("[DEBUG] Possible temporary Amazon blocking page")

    blocks = extract_offer_blocks(driver)
    print(f"[DEBUG] Total offer blocks found: {len(blocks)}")

    rank = 1

    for idx, block in enumerate(blocks, start=1):
        try:
            block_text = clean_text(block.text)
            print(f"[DEBUG] Block #{idx} preview: {block_text[:300]}")

            condition_text = extract_offer_condition(block)
            print(f"[DEBUG] Block #{idx} condition: {condition_text}")

            if not is_new_offer(condition_text):
                print(f"[DEBUG] Block #{idx} skipped: not new")
                continue

            seller_name = extract_offer_seller(block)
            print(f"[DEBUG] Block #{idx} seller: {seller_name}")

            if not seller_name:
                print(f"[DEBUG] Block #{idx} skipped: seller missing")
                continue

            if is_excluded_seller(seller_name):
                print(f"[DEBUG] Block #{idx} skipped: excluded seller")
                continue

            price_item = extract_offer_price(block)
            print(f"[DEBUG] Block #{idx} item price: {price_item}")

            if price_item is None:
                print(f"[DEBUG] Block #{idx} skipped: price missing")
                continue

            shipping_cost, shipping_text = extract_offer_shipping(block)
            print(f"[DEBUG] Block #{idx} shipping text: {shipping_text}")
            print(f"[DEBUG] Block #{idx} shipping cost: {shipping_cost}")

            total_price = price_item + (shipping_cost or 0.0)

            rows.append(
                {
                    "scrape_timestamp_utc": scrape_ts,
                    "batch_id": batch_id,
                    "brand": brand,
                    "asin": asin,
                    "product_name": product_name,
                    "product_url": product_url,
                    "offers_url": offers_url,
                    "seller_name": seller_name,
                    "offer_condition": clean_text(condition_text),
                    "price_item_eur": price_item,
                    "shipping_eur": shipping_cost if shipping_cost is not None else "",
                    "price_total_eur": total_price,
                    "is_free_shipping": 1 if shipping_cost == 0 else 0,
                    "is_main_offer": 0,
                    "offer_rank": rank,
                    "scrape_status": "ok",
                }
            )
            rank += 1

        except Exception as e:
            print(f"[DEBUG] Error while parsing block #{idx}: {e}")
            continue

    print(f"[DEBUG] Final valid new offers extracted: {len(rows)}")
    return rows


def deduplicate_rows(rows: List[Dict]) -> List[Dict]:
    seen = set()
    deduped = []

    for row in rows:
        key = (
            row["asin"],
            row["seller_name"].strip().lower(),
            row["offer_condition"].strip().lower(),
            row["price_total_eur"],
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    return deduped


def scrape_product(
    driver: webdriver.Chrome,
    brand: str,
    product: Dict,
    batch_id: int,
) -> List[Dict]:
    product_url = product["product_url"]
    asin = product.get("asin") or extract_asin_from_url(product_url)
    product_name = product.get("product_name", "")
    scrape_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    if not asin:
        return [
            {
                "scrape_timestamp_utc": scrape_ts,
                "batch_id": batch_id,
                "brand": brand,
                "asin": "",
                "product_name": product_name,
                "product_url": product_url,
                "offers_url": "",
                "seller_name": "",
                "offer_condition": "",
                "price_item_eur": "",
                "shipping_eur": "",
                "price_total_eur": "",
                "is_free_shipping": "",
                "is_main_offer": "",
                "offer_rank": "",
                "scrape_status": "error_asin_missing",
            }
        ]

    all_rows = []

    try:
        main_rows = scrape_main_offer(
            driver=driver,
            brand=brand,
            asin=asin,
            product_name=product_name,
            product_url=product_url,
            scrape_ts=scrape_ts,
            batch_id=batch_id,
        )
        all_rows.extend(main_rows)
    except Exception:
        pass

    try:
        listing_rows = scrape_offer_listing(
            driver=driver,
            brand=brand,
            asin=asin,
            product_name=product_name,
            product_url=product_url,
            scrape_ts=scrape_ts,
            batch_id=batch_id,
        )
        all_rows.extend(listing_rows)
    except Exception:
        pass

    all_rows = deduplicate_rows(all_rows)

    if not all_rows:
        return [
            {
                "scrape_timestamp_utc": scrape_ts,
                "batch_id": batch_id,
                "brand": brand,
                "asin": asin,
                "product_name": product_name,
                "product_url": product_url,
                "offers_url": f"https://www.amazon.fr/gp/offer-listing/{asin}",
                "seller_name": "",
                "offer_condition": "",
                "price_item_eur": "",
                "shipping_eur": "",
                "price_total_eur": "",
                "is_free_shipping": "",
                "is_main_offer": "",
                "offer_rank": "",
                "scrape_status": "no_new_offers_found",
            }
        ]

    return all_rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to config JSON")
    parser.add_argument(
        "--headful",
        action="store_true",
        help="Launch browser with UI instead of headless mode",
    )
    args = parser.parse_args()

    config = load_json(args.config)
    brand = config["brand"]
    output_csv = config["output_csv"]
    state_file = config["state_file"]
    products = config["products"]

    state = load_state(state_file)
    batch_id = int(state.get("last_batch_id", -1)) + 1

    driver = build_driver(headless=not args.headful)

    try:
        driver.get("https://www.amazon.fr")
        accept_or_reject_cookies(driver)

        all_rows = []

        for product in products:
            rows = scrape_product(
                driver=driver,
                brand=brand,
                product=product,
                batch_id=batch_id,
            )
            all_rows.extend(rows)
            time.sleep(2)

        append_rows_to_csv(output_csv, all_rows)

        new_state = {
            "last_batch_id": batch_id,
            "last_run_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "last_status": "ok",
            "run_count": int(state.get("run_count", 0)) + 1,
        }
        save_json(state_file, new_state)

        print(f"[OK] {len(all_rows)} lignes ajoutées dans {output_csv}")

    except Exception as e:
        error_state = {
            "last_batch_id": state.get("last_batch_id", -1),
            "last_run_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
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
