import asyncio
import random
import aiohttp
from bs4 import BeautifulSoup
from typing import List, Dict
from db import init_db, insert_lot, lot_exists

BASE_URL = "https://med.ecc.kz"
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
]
REQUEST_DELAY = (0.1, 0.3)

class Parser:
    def __init__(self):
        self.session = None

    async def __aenter__(self):
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        self.session = aiohttp.ClientSession(headers=headers)
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    async def fetch(self, url: str):
        try:
            async with self.session.get(url, timeout=20) as r:
                r.raise_for_status()
                html = await r.text()
                return BeautifulSoup(html, "html.parser")
        except:
            return None

    async def parse_page(self, page: int) -> List[Dict]:
        url = f"{BASE_URL}/searchanno?page={page}"
        soup = await self.fetch(url)
        if not soup:
            return []
        table = soup.find("table", class_="table")
        if not table:
            return []
        announcements = []
        for row in table.find_all("tr")[1:]:
            cols = row.find_all("td")
            if len(cols) < 9:
                continue
            try:
                ann_id = cols[0].text.strip()
                ann = {
                    "ann_id": ann_id,
                    "customer": cols[1].text.strip(),
                    "title": cols[2].text.strip(),
                    "method": cols[3].text.strip(),
                    "type": cols[4].text.strip(),
                    "date_start": cols[5].text.strip(),
                    "date_end": cols[6].text.strip(),
                    "lots": int(cols[7].text.strip()),
                    "amount": float(cols[8].text.strip().replace(" ", "")),
                    "status": cols[9].text.strip(),
                    "link": BASE_URL + cols[2].find("a")["href"]
                }
                # --- Парсим Кол-во лотов из Общих сведений ---
                url_info = f"{BASE_URL}/ru/announce/index/{ann_id}"
                soup_info = await self.fetch(url_info)
                await asyncio.sleep(random.uniform(*REQUEST_DELAY))
                def extract_field(label):
                    cell = soup_info.find("td", string=lambda t: t and label in t)
                    if cell and cell.find_next_sibling("td"):
                        return cell.find_next_sibling("td").text.strip()
                    return ""
                lots_count_info = extract_field("Кол-во лотов в объявлении")
                if lots_count_info.isdigit():
                    ann["lots_count_info"] = int(lots_count_info)
                else:
                    ann["lots_count_info"] = None
                announcements.append(ann)
            except Exception as e:
                print("Ошибка парсинга объявления:", e)
                continue
        return announcements

    async def parse_lots(self, ann: Dict) -> List[Dict]:
        result = []
        page = 1
        max_pages = 20  # Ограничим максимальное количество страниц лотов
        while page <= max_pages:
            print(f"[{ann['ann_id']}] Парсинг лотов, страница {page} (уже собрано {len(result)})")
            url = f"{ann['link']}?tab=lots&page={page}"
            soup = await self.fetch(url)
            await asyncio.sleep(random.uniform(*REQUEST_DELAY))
            if not soup:
                print(f"[{ann['ann_id']}] Нет ответа/ошибка на странице {page} — остановка.")
                break
            table = soup.find("table", class_="table-striped")
            if not table:
                print(f"[{ann['ann_id']}] Нет таблицы на странице {page} — остановка.")
                break
            rows = table.find_all("tr")[1:]
            if not rows:
                print(f"[{ann['ann_id']}] Нет строк лотов на странице {page} — остановка.")
                break
            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 9:
                    continue
                try:
                    lot = {
                        "plan_point_id": cols[0].text.strip(),
                        "lot_id": ann["ann_id"] + "-" + cols[0].text.strip(),
                        "ann_id": ann["ann_id"],
                        "title": cols[2].text.strip(),
                        "customer": cols[1].text.strip(),
                        "description": cols[3].text.strip(),
                        "item_type": cols[4].text.strip(),
                        "unit": cols[5].text.strip(),
                        "quantity": float(cols[6].text.strip()),
                        "price": float(cols[7].text.strip().replace(" ", "")),
                        "amount": float(cols[8].text.strip().replace(" ", "")),
                        "date_start": ann["date_start"],
                        "date_end": ann["date_end"],
                        "method": ann["method"],
                        "status": ann["status"]
                    }
                    result.append(lot)
                except Exception as e:
                    print(f"[{ann['ann_id']}] Ошибка парсинга лота на стр {page}:", e)
                    continue
            # Условие выхода — если собрали все лоты по инфо или достигли макс. страниц
            if ann.get("lots_count_info") and len(result) >= ann["lots_count_info"]:
                print(f"[{ann['ann_id']}] Достигли нужного количества лотов: {len(result)} из {ann['lots_count_info']}")
                break
            page += 1
        if page > max_pages:
            print(f"[{ann['ann_id']}] Достигнут лимит страниц лотов ({max_pages}), собрано {len(result)}")
        return result


async def run_parser(pages: int, progress_callback=None):
    init_db()
    new_lots = []
    async with Parser() as parser:
        for page in range(1, pages + 1):
            anns = await parser.parse_page(page)
            lots_lists = await asyncio.gather(
                *[parser.parse_lots(ann) for ann in anns]
            )
            for lots in lots_lists:
                for lot in lots:
                    if not lot_exists(lot["lot_id"]):
                        insert_lot(lot)
                        new_lots.append(lot)
            if progress_callback:
                progress_callback(page, pages, len(new_lots))
            await asyncio.sleep(0.5)
    return new_lots

if __name__ == "__main__":
    import sys
    pages = int(sys.argv[1]) if len(sys.argv) > 1 else 1

    def cb(page, total, added):
        print(f"Страница {page}/{total}. Новых лотов: {added}")

    asyncio.run(run_parser(pages, cb))
