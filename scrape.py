import time
import os
import threading
import requests
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import ddddocr

ocr = ddddocr.DdddOcr()
ocr_lock = threading.Lock()

def solve_captcha(image_bytes):
    with ocr_lock:
        return ocr.classification(image_bytes, png_fix=True)

def print_records(sid, records):
    print(f"\n{'─' * 50}")
    print(f"  SBD: {sid}")
    print(f"{'─' * 50}")
    for record in records:
        for key, val in record.items():
            print(f"  {key:<20} {val}")
        print()
    print(f"{'─' * 50}\n")


def fetch_score(search_page_url: str,
                base_headers: dict,
                sid: str,
                max_retries: int = 9999):

    session = requests.Session()
    headers = dict(base_headers)

    r0 = session.get(search_page_url, headers=headers, timeout=30)
    r0.raise_for_status()

    def get_val(name):
        return "";

    tokens = {
        'securityToken': get_val("securityToken"),
        'submitFormId':  "1016",
        'moduleId':      "1016",
        'itemId':        "69cda4b602f04a9fca0fecc6" #hsg 10 2025-2026
    }

    auth = session.cookies.get("AUTH_BEARER_default")
    if auth:
        headers["Cookie"] = f"AUTH_BEARER_default={auth}"

    for _ in range(max_retries):
        ts = str(int(time.time() * 1000))
        captcha_url = (
            f"https://hatinh.edu.vn/api/Common/Captcha/getCaptcha"
            f"?returnType=image&site=32982&width=150&height=50&t={ts}"
        )
        capcha_req = session.get(captcha_url,
                         headers={**headers, "Referer": search_page_url},
                         timeout=30)
        capcha_req.raise_for_status()

        try:
            code = solve_captcha(capcha_req.content)
        except Exception as e:
            print(f"[{sid}] OCR error: {e}")
            continue

        params = {
            "module": "Content.Listing",
            "moduleId": tokens['moduleId'],
            "cmd": "redraw",
            "site": "32982",
            "url_mode": "rewrite",
            "submitFormId": tokens['submitFormId'],
            "layout": "Decl.DataSet.Detail.default",
            "itemsPerPage": "1000",
            "pageNo": "1",
            "service": "Content.Decl.DataSet.Grouping.select",
            "itemId": tokens['itemId'],
            "gridModuleParentId": "17",
            "type": "Decl.DataSet",
            "keyword": sid,
            "viewPassword": "1",
            "BDC_UserSpecifiedCaptchaId": code,
            "captcha_check": code,
            "captcha_code": code,
            "_t": ts,
            "securityToken": tokens['securityToken']
        }

        score_req = session.get(
            "https://hatinh.edu.vn/",
            headers={**headers, "X-Requested-With": "XMLHttpRequest"},
            params=params,
            timeout=30
        )
        if score_req.status_code != 200:
            continue

        soup = BeautifulSoup(score_req.text, "html.parser")

        # check "not found" message
        no_data_div = soup.select_one(".no-data-title-filter")
        if no_data_div and "Không tìm thấy kết quả tìm kiếm" in no_data_div.get_text(strip=True):
            print(f"[{sid}] not found")
            return None

        tables = soup.select(".cont-dataset-detail table")
        if not tables:
            continue

        records = []
        for tbl in tables:
            cols = [th.get_text(strip=True) for th in tbl.select("thead th")]
            for row in tbl.select("tbody tr"):
                vals = [td.get_text(strip=True) for td in row.select("td")]
                records.append(dict(zip(cols, vals)))

        return records

    print(f"[{sid}] All {max_retries} attempts failed.")
    return None

def worker(sid, search_page_url, base_headers):
    try:
        records = fetch_score(search_page_url, base_headers, sid)
        return sid, records
    except Exception as e:
        print(f"[{sid}] Unhandled exception: {e}")
        return sid, None

def run_batch(ids_to_fetch, search_page_url, base_headers,
              excel_file, failed_file):
    if os.path.exists(excel_file):
        df_existing = pd.read_excel(excel_file)
    else:
        df_existing = pd.DataFrame()

    df_lock   = threading.Lock()
    fail_lock = threading.Lock()
    MAX_WORKERS = 5

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(worker, sid, search_page_url, base_headers): sid
            for sid in ids_to_fetch
        }
        for future in as_completed(futures):
            sid, records = future.result()
            if records:
                with df_lock:
                    df_existing = pd.concat(
                        [df_existing, pd.DataFrame(records)],
                        ignore_index=True, sort=False
                    )
                    df_existing.to_excel(excel_file, index=False)
                print_records(sid, records)
            else:
                with fail_lock:
                    with open(failed_file, "a", encoding="utf-8") as fout:
                        fout.write(sid + "\n")
                print(f"[{sid}] failed — logged to {failed_file}")

def main():
    search_page_url = "https://hatinh.edu.vn/tracuudiemthihsg"
    base_headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0 Safari/537.36"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": search_page_url
    }

    sbd_file    = "sbd_list.txt"
    excel_file  = "scores_all.xlsx"
    failed_file = "failed_ids.txt"

    if not os.path.exists(sbd_file):
        print(f"'{sbd_file}' not found.")
        return

    with open(sbd_file, "r", encoding="utf-8") as f:
        all_ids = [line.strip() for line in f if line.strip()]

    if not all_ids:
        print(f"'{sbd_file}' is empty.")
        return

    print(f"Loaded {len(all_ids)} SBD(s) from '{sbd_file}'.")

    while True:
        # find missing IDs
        if os.path.exists(excel_file):
            df_saved = pd.read_excel(excel_file)
            saved_ids = set(df_saved.iloc[:, 0].astype(str))
        else:
            saved_ids = set()

        missing = [sid for sid in all_ids if sid not in saved_ids]

        if not missing:
            print("All IDs have been saved. Done.")
            break

        print(f"{len(missing)} ID(s) not yet in saved file.")
        answer = input("Fetch missing IDs? [y/n]: ").strip().lower()
        if answer != "y":
            print("Exiting.")
            break

        # clear failed log
        if os.path.exists(failed_file):
            os.remove(failed_file)

        run_batch(missing, search_page_url, base_headers, excel_file, failed_file)


if __name__ == "__main__":
    main()