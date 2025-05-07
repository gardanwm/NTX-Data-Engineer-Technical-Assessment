# -*- coding: utf-8 -*-
"""Soal 3 - Data Collection Case

# -*- coding: utf-8 -*-
"""Web scraper for FortiGuard encyclopedia"""

# Import dan mount Google Drive
from google.colab import drive
drive.mount('/content/drive/')

# Instalasi package eksternal (run di sel terpisah di Colab)
pip install httpx BeautifulSoup4 polars tqdm
pip install nest_asyncio

# Import library yang dibutuhkan
import asyncio
import httpx
from bs4 import BeautifulSoup
import polars as pl
import json
import os
import random
import time
from tqdm.asyncio import tqdm
import nest_asyncio

# Konfigurasi agar asyncio bisa berjalan di Colab
nest_asyncio.apply()

# ========================== KONFIGURASI DASAR ==========================
BASE_URL = "https://www.fortiguard.com"
LEVELS = [1, 2, 3, 4, 5]  # Level risiko IPS yang akan discan
MAX_PAGES = [7, 34, 181, 451, 288]  # Jumlah halaman per level
DATASETS_DIR = "datasets"
os.makedirs(DATASETS_DIR, exist_ok=True)

# Pengaturan scraping
TIMEOUT = 20
CONCURRENCY_LIMIT = 5
DELAY_RANGE = (1, 2)
BATCH_SIZE = 10

# Header untuk menyamarkan request sebagai browser biasa
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
}

# ========================== TRACKING PROGRESS ==========================
skipped_pages = {str(level): [] for level in LEVELS}
completed_files = set()

def check_existing_files():
    """Cek file yang sudah selesai sebelumnya"""
    for level in LEVELS:
        file_path = f"{DATASETS_DIR}/fortilists{level}.csv"
        if os.path.exists(file_path):
            completed_files.add(level)
            print(f"Level {level} already exists, continuing to next level.")

# ========================== FETCH HTML ==========================
async def fetch(url, session=None):
    """Ambil HTML dari URL dengan error handling"""
    close_session = False
    if session is None:
        session = httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS, follow_redirects=True)
        close_session = True

    async with semaphore:
        try:
            response = await session.get(url)
            response.raise_for_status()

            content = response.text
            if "Access Denied" in content or "Robot or Virus Scan" in content:
                print(f"Warning: Possible access denied for {url}.")
                return None

            await asyncio.sleep(random.uniform(*DELAY_RANGE))
            return content

        except (httpx.HTTPError, httpx.RequestError, httpx.ReadTimeout) as e:
            print(f"Error fetching {url}: {str(e)}.")
            return None

        finally:
            if close_session:
                await session.aclose()

# ========================== SCRAPER FUNGSI SPESIFIK ==========================
async def extract_detail_url(row):
    """Ambil link detail dari baris onclick"""
    try:
        onclick = row.attrs.get("onclick")
        if onclick:
            partial_link = onclick.split("'")[1]
            return BASE_URL + partial_link
    except (IndexError, ValueError) as e:
        print(f"Error parsing onclick: {e}")
    return None

async def scrape_detail(url, session):
    """Scrape halaman detail dan ambil judul"""
    html = await fetch(url, session)
    if html is None:
        return {"title": "N/A", "link": url}

    soup = BeautifulSoup(html, "html.parser")
    title = "N/A"

    # Metode pencarian judul (berlapis)
    title_tag = soup.find("h1", class_="title")
    if title_tag:
        text_node = title_tag.get_text(strip=True)
        if text_node:
            title = text_node

    if title == "N/A":
        info_box = soup.select_one(".info-box h2")
        if info_box:
            title = info_box.get_text(strip=True)

    if title == "N/A":
        id_title = soup.select_one("#main-title")
        if id_title:
            title = id_title.get_text(strip=True)

    if title == "N/A":
        meta_title = soup.find("meta", property="og:title")
        if meta_title:
            title = meta_title.get("content", "N/A")

    return {"title": title, "link": url}

# ========================== SCRAPE PER PAGE ==========================
async def scrape_page(level, page, session):
    """Scrape satu halaman listing berdasarkan level dan page"""
    url = f"{BASE_URL}/encyclopedia?type=ips&risk={level}&page={page}"
    html = await fetch(url, session)

    if html is None:
        skipped_pages[str(level)].append(page)
        return []

    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("section.table-body div.row[onclick]")

    # Jika selector utama tidak ada, coba alternatif
    if not rows:
        print(f"Warning: No rows found on level {level}, page {page}. Checking alternative selectors...")
        rows = soup.select("div.row[onclick], tr[onclick], .item[onclick]")
        if not rows:
            print(f"Still no rows found for level {level}, page {page}. Marking as skipped.")
            skipped_pages[str(level)].append(page)
            return []

    print(f"Found {len(rows)} items on level {level}, page {page}")

    detail_urls = []
    for row in rows:
        detail_url = await extract_detail_url(row)
        if detail_url:
            detail_urls.append(detail_url)

    if not detail_urls:
        skipped_pages[str(level)].append(page)
        return []

    # Jalankan scraping detail secara paralel
    tasks = [scrape_detail(url, session) for url in detail_urls]
    results = await asyncio.gather(*tasks)
    return results

# ========================== SCRAPE PER LEVEL ==========================
async def scrape_level_in_batches(level, max_page):
    """Scrape seluruh halaman pada satu level secara batch"""
    if level in completed_files:
        print(f"Skipping level {level} as it's already completed.")
        return []

    all_data = []
    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS, follow_redirects=True) as session:
        for start_page in range(1, max_page + 1, BATCH_SIZE):
            end_page = min(start_page + BATCH_SIZE - 1, max_page)
            batch_tasks = [scrape_page(level, page, session) for page in range(start_page, end_page + 1)]

            with tqdm(total=len(batch_tasks), desc=f"Level {level} Batch {start_page}-{end_page}", ncols=100) as pbar:
                for task in asyncio.as_completed(batch_tasks):
                    page_data = await task
                    all_data.extend(page_data)
                    pbar.update(1)

                    # Simpan hasil sementara
                    if len(all_data) % 100 == 0 and all_data:
                        partial_df = pl.DataFrame(all_data)
                        partial_df.write_csv(f"{DATASETS_DIR}/fortilists{level}_partial.csv")

            # Simpan halaman yang dilewati
            with open(f"{DATASETS_DIR}/skipped.json", "w") as f:
                json.dump(skipped_pages, f, indent=2)

    return all_data

# ========================== MAIN FUNCTION ==========================
async def main():
    """Fungsi utama yang menjalankan seluruh proses"""
    global semaphore
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    start_time = time.time()

    check_existing_files()

    skipped_file = f"{DATASETS_DIR}/skipped.json"
    if os.path.exists(skipped_file):
        with open(skipped_file, "r") as f:
            try:
                skipped_data = json.load(f)
                skipped_pages.update(skipped_data)
                print(f"Loaded skipped pages: {skipped_pages}")
            except json.JSONDecodeError:
                print("Error reading skipped.json, starting fresh.")

    for level, max_page in zip(LEVELS, MAX_PAGES):
        if level not in completed_files:
            data = await scrape_level_in_batches(level, max_page)

            if data:
                df = pl.DataFrame(data)
                df = df.unique(subset=["link"], maintain_order=True)
                output_file = f"{DATASETS_DIR}/fortilists{level}.csv"
                df.write_csv(output_file)
                print(f"Saved {len(df)} records to {output_file}")

                partial_file = f"{DATASETS_DIR}/fortilists{level}_partial.csv"
                if os.path.exists(partial_file):
                    os.remove(partial_file)

    # Simpan hasil halaman yang dilewati
    with open(f"{DATASETS_DIR}/skipped.json", "w") as f:
        json.dump(skipped_pages, f, indent=2)

    # Statistik akhir
    print("\nScraping Statistics:")
    total_skipped = sum(len(pages) for pages in skipped_pages.values())
    print(f"Total skipped pages: {total_skipped}")

    for level in LEVELS:
        file_path = f"{DATASETS_DIR}/fortilists{level}.csv"
        if os.path.exists(file_path):
            df = pl.read_csv(file_path)
            na_count = df.filter(pl.col("title") == "N/A").height
            total_count = df.height
            na_percentage = (na_count / total_count * 100) if total_count > 0 else 0
            print(f"Level {level}: {total_count} records, {na_count} N/A titles ({na_percentage:.2f}%)")

    elapsed_time = time.time() - start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    print(f"Scraping completed in {int(hours)}h {int(minutes)}m {seconds:.2f}s.")

# Eksekusi fungsi utama
if __name__ == "__main__":
    asyncio.run(main())









