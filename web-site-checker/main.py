import time
import asyncio
from pathlib import Path

import aiohttp
import pandas as pd
from tqdm.asyncio import tqdm_asyncio


ROOT_DIR = Path(__file__).parent
KEY = "Сайт"
VKEY = "Преобразованный сайт"


HEADERS = {
    "Accept": "text/html",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
}


def get_sites() -> pd.DataFrame:
    return pd.read_excel(ROOT_DIR / "Sushi.xlsx")


RESPONSE_TIMEOUT = aiohttp.ClientTimeout(total=18)
PREFETCH_CHECKOUT = ["onelink.to", "bit.ly", "clck.ru", "taplink.cc", "vk.cc"]
SEMAPHORE = asyncio.Semaphore(50)


async def prefetch(url: str) -> str:
    if "http" not in url:
        url = "http://" + url

    try:
        async with SEMAPHORE:
            async with aiohttp.ClientSession(
                headers=HEADERS, timeout=RESPONSE_TIMEOUT
            ) as session:
                async with session.get(url) as response:
                    final_url = str(response.url)
                    if "https://vk.com/away.php" in final_url:
                        final_url = final_url.split("&to=")[-1]
                    return final_url

    except Exception as ex:
        print("Prefetch Error", type(ex), vars(ex))
        return None


async def fetch(url) -> tuple:
    if url is None:
        return None, None, None, None

    try:
        async with SEMAPHORE:
            async with aiohttp.ClientSession(
                headers=HEADERS, timeout=RESPONSE_TIMEOUT
            ) as session:
                start = time.time()

                async with session.get(url) as response:
                    resp_time = time.time() - start
                    code = response.status
                    html = await response.text()
                    html_len = len(html)
                    is_tilda = "tilda.ws" in html or "tildacdn.com" in html

                    return code, html_len, is_tilda, resp_time

    except Exception as ex:
        print("Fetch Error", type(ex), vars(ex))
        return None


async def fetch_https(url):
    if url is None:
        return None

    if "http://" in url:
        url = url.replace("http://", "https://")

    try:
        async with SEMAPHORE:
            async with aiohttp.ClientSession(
                headers=HEADERS, timeout=RESPONSE_TIMEOUT
            ) as session:
                async with session.get(url) as response:
                    return True

    except Exception as ex:
        print("Fetch HTTPS Error", type(ex), vars(ex))
        return None


async def main():
    sites_df = get_sites()

    sites = sites_df[KEY]

    prefetch_tasks = [prefetch(s) for s in sites]
    sites_urls = await tqdm_asyncio.gather(*prefetch_tasks)

    sites_df[VKEY] = sites_urls

    stat_tasks = [fetch(s) for s in sites_urls]
    stats = await tqdm_asyncio.gather(*stat_tasks)

    sites_df["resp_code"] = [r[0] for r in stats]
    sites_df["html_len"] = [r[1] for r in stats]
    sites_df["is_tilda"] = [r[2] for r in stats]
    sites_df["resp_time"] = [r[3] for r in stats]

    https_fetch = [fetch_https(s) for s in sites_urls]
    https_check = await tqdm_asyncio.gather(*https_fetch)

    sites_df["https_checkout"] = https_check

    sites_df.to_excel(ROOT_DIR / "output.xlsx", index=False)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
