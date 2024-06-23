import math
import time
import json
from pathlib import Path
from enum import Enum

import pandas as pd
import undetected_chromedriver as uc
from bs4 import BeautifulSoup as soup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

ROOT_DIR = Path(__file__).parent

CONFIG = ROOT_DIR / "config"
QUERIES = ROOT_DIR / "queries"
OUTPUT = ROOT_DIR / "output"

CITY = "Арзамас"
REGIONS = [11080]

### Config
TIMEOUT = 2
MAX_TRIES = 2
GOOGLE_KF = 0.6
OFFER_KF = 0.3
LAST_MONTHS = 9

QUERY = "Запрос"
LINK = "Оригинальная ссылка"
STATS_DATE = "Дата"
STATS = "Кол-во запросов"
HYPERLINK = "Источник"

YANDEX = "Yandex"
GOOGLE = "Google"


class MonthStat:
    def __init__(
        self,
        date: str,
        count: int,
        percent: float,
    ) -> None:
        self.date = date
        self.count = int(count)
        self.percent = float(percent)

    def __repr__(self) -> str:
        return f"{self.date}:{self.count}"


class QueryType(Enum):
    all = "all"
    special = "special"


class AggType(Enum):
    all_stat = "all_stat"
    last = "last"
    max = "max"


class StatConfig:
    def __init__(
        self,
        agg_type: AggType,
    ):
        self.agg_type = agg_type

    def agg(self, stats: list[MonthStat]) -> list[MonthStat]:
        if self.agg_type is AggType.all_stat:
            return stats
        elif self.agg_type is AggType.last:
            return stats[len(stats) - 1 :]
        elif self.agg_type is AggType.max:
            return [max(stats, key=lambda x: x.count)]
        else:
            raise ValueError("AggType is not valid")


class Query:
    def __init__(
        self,
        query: str,
        type: QueryType,
        regions: tuple[int] | None = None,
    ) -> None:
        self.query = query
        self.type = type
        self.regions = tuple(regions) if regions else None

        self.reg_sep = r"%2C"
        self.word_sep = r"%20"

    @property
    def link(self) -> str:
        link = "https://wordstat.yandex.ru/?"
        if self.regions:
            link += "region=" + self.reg_sep.join(map(str, self.regions))
        else:
            link += "region=all"

        link += "&view=graph"
        link += "&words=" + self.word_sep.join(self.query.split(" "))
        return link

    def __hash__(self):
        return hash((self.query, self.regions))

    def __eq__(self, other):
        return isinstance(other, self) and self.__hash__() == other.__hash__()


class Wordstat:
    def __init__(
        self,
        stat_config: StatConfig,
    ) -> None:
        self.driver = None

        self.stat_config = stat_config

        self.all: dict[Query, list[MonthStat]] = {}
        self.special: dict[Query, list[MonthStat]] = {}

    def _set_cookies(self):
        self.driver.get("https://yandex.ru")
        time.sleep(5)

        with open(CONFIG / "yandex.cookie.json", "r") as file:
            cookies = json.load(file)
            for cookie in cookies:
                if "sameSite" in cookie:
                    del cookie["sameSite"]

                    self.driver.execute_cdp_cmd("Network.setCookie", cookie)

    def parse_stat(
        self,
        html: str,
        data: list,
    ) -> list[MonthStat]:
        s = soup(html, "lxml")

        table = s.find("table", {"class": "table__wrapper"}).find("tbody")
        rows = table.find_all("tr")

        for row in rows:
            cells = row.find_all("td")
            data.append(
                MonthStat(
                    cells[0].text.strip(),
                    cells[1].text.strip().replace(" ", ""),
                    cells[2].text.strip().replace(",", "."),
                )
            )

        return data

    def get_stat(
        self,
        query: Query,
        save: bool = True,
        ntry: int = 0,
    ) -> list[MonthStat]:
        if not self.driver:
            raise ValueError("Driver is not initialized")

        time.sleep(TIMEOUT)

        data = []
        try:
            self.driver.get(query.link)

            WebDriverWait(self.driver, 10).until(
                EC.visibility_of_element_located((By.CLASS_NAME, "table__wrapper"))
            )
            time.sleep(1)

            html = self.driver.page_source
            data = self.parse_stat(html, data)

            if save:
                self.save(query, data)

            return data

        except TimeoutException:
            ntry += 1
            if ntry >= MAX_TRIES:
                print("END TRIES")
                return data
            else:
                return self.get_stat(query, save, ntry)

    def save(
        self,
        query: Query,
        data: list[MonthStat],
    ) -> None:
        if query.type is QueryType.all:
            self.all[query] = data
        elif query.type is QueryType.special:
            self.special[query] = data
        else:
            raise ValueError("Query type is not valid")

    def _get_dataframe(
        self,
        type: QueryType,
    ) -> pd.DataFrame:
        dataframe = pd.DataFrame()

        source = self.all if type is QueryType.all else self.special

        index = 0
        for query, stats in source.items():
            stats = stats[len(stats) - LAST_MONTHS :]
            stats = self.stat_config.agg(stats)

            for stat in stats:
                dataframe.at[index, LINK] = query.link
                dataframe.at[index, QUERY] = query.query
                dataframe.at[index, STATS_DATE] = stat.date
                dataframe.at[index, STATS] = stat.count

                index += 1

        return dataframe.sort_values(STATS, ascending=False)

    def make_hyperlink(self, row: pd.Series) -> pd.Series:
        row[HYPERLINK] = rf'<a href="{row[LINK]}">Ссылка</a>'
        return row

    def to_dataframe(self) -> list[pd.DataFrame]:
        all_df = self._get_dataframe(QueryType.all)
        special_df = self._get_dataframe(QueryType.special)
        return all_df, special_df

    def save_dataframe(self) -> None:
        all_df, special_df = self.to_dataframe()
        with pd.ExcelWriter(OUTPUT / "stats.xlsx") as writer:
            all_df.to_excel(writer, sheet_name="Общие запросы", index=False)
            special_df.to_excel(writer, sheet_name="Запросы по городу", index=False)

    def save_html(self) -> None:
        all_df, special_df = self.to_dataframe()

        yandex_all_df = all_df.apply(self.make_hyperlink, axis=1)
        yandex_all_df[YANDEX] = yandex_all_df[STATS].astype(int)
        yandex_all_df[GOOGLE] = (yandex_all_df[STATS] * GOOGLE_KF).astype(int)

        yandex_special_df = special_df.apply(self.make_hyperlink, axis=1)
        yandex_special_df[YANDEX] = yandex_special_df[STATS].astype(int)
        yandex_special_df[GOOGLE] = (yandex_special_df[STATS] * GOOGLE_KF).astype(int)

        COLUMNS = [QUERY, STATS_DATE, YANDEX, GOOGLE, HYPERLINK]

        yandex_all_df[COLUMNS].to_html(
            OUTPUT / "all_stats.html", index=False, escape=False
        )
        yandex_special_df[COLUMNS].to_html(
            OUTPUT / "special_stats.html", index=False, escape=False
        )

        atAll = sum(
            [
                df[YANDEX].sum() + df[GOOGLE].sum()
                for df in [
                    yandex_all_df,
                    yandex_special_df,
                ]
            ]
        )

        print("Всего количество запросов:", atAll)
        print("Количество запросов для оффера", atAll * OFFER_KF)

    def __enter__(self):
        self.driver = uc.Chrome(headless=False, use_subprocess=False)
        self._set_cookies()
        return self

    def __exit__(self, *args, **kwargs):
        if self.driver:
            self.driver.close()
            self.driver.quit()
            del self.driver

            self.driver = None


def read_queries(path: str | Path) -> list[str]:
    with open(path, "r") as file:
        return [row.strip() for row in file.readlines()]


def get_queries(
    regions: list[int],
    mixit: bool = True,
) -> tuple[list[Query]]:
    all = read_queries(QUERIES / "all.txt")
    special = read_queries(QUERIES / "special.txt")

    if mixit:
        all = [
            f"{s} {CITY}" if CITY not in special else s for s in special if s not in all
        ]

    return [Query(q, QueryType.all) for q in all], [
        Query(q, QueryType.special, regions=regions) for q in special
    ]


if __name__ == "__main__":
    qall, qspec = get_queries(REGIONS)

    wordstat = Wordstat(StatConfig(AggType.max))

    with wordstat as engine:
        for query in qall:
            engine.get_stat(query)

        for query in qspec:
            engine.get_stat(query)

    wordstat.save_html()
