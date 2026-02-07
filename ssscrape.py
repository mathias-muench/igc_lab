#!/usr/bin/env python3

import argparse
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from typing import Iterator, Tuple
import requests_cache
import locale
from unidecode import unidecode

BASE_URL = "https://www.soaringspot.com"


class SoaringSpotScraper(requests_cache.CachedSession):
    def __init__(self):
        super().__init__()
        locale.setlocale(locale.LC_ALL, "en_US.UTF-8")
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

    def get_soup(self, url: str) -> BeautifulSoup:
        response = self.get(url, headers=self.headers)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

    def get_task_days(self, competition_url: str) -> list[str]:
        soup = self.get_soup(competition_url)
        task_links = []
        for link in soup.select('table a[href*="/task-"]'):
            href = link.get("href")
            if href and href not in task_links:
                task_links.append(href)
        return [urljoin(BASE_URL, link) for link in task_links]

    def get_pilot_igc_links(
        self, task_day_url: str
    ) -> Iterator[Tuple[str, str, str, str, str]]:
        soup = self.get_soup(task_day_url)

        date_match = re.search(r"-\d+-on-(\d{4}-\d{2}-\d{2})/daily", task_day_url)
        date_str = date_match.group(1)

        for row in soup.select("table tbody tr"):
            cols = row.find_all("td")
            start_time = cols[7].get_text(strip=True)
            finish_time = cols[8].get_text(strip=True)

            iso_start = f"{date_str}T{start_time}" if start_time else ""
            iso_finish = f"{date_str}T{finish_time}" if finish_time else ""

            if start_time:
                download_link = row.find("a")
                data_content = download_link["data-content"]
                soup = BeautifulSoup(data_content, "html.parser")
                a_tags = soup.find_all("a")
                igc_url = urljoin(BASE_URL, a_tags[1]["href"])
            else:
                igc_url = ""

            contestant = cols[3].get_text(strip=True)
            points = locale.atoi(cols[-1].get_text(strip=True))
            yield igc_url, iso_start, iso_finish, contestant, points

    @staticmethod
    def append_times_to_igc(
        igc_content: bytes,
        start_time: str,
        finish_time: str,
        contestant: str,
        points: str,
    ) -> bytes:
        new_content = f"LSCR::START:{start_time}\r\nLSCR::FINISH:{finish_time}\r\n"
        new_content += f"LSCR::CONTESTANT:{contestant}\r\n"
        new_content += f"LSCR::POINTS:{points}\r\n"
        return igc_content + unidecode(new_content).encode("ascii")

    def download_igc_data(self, url: str) -> Tuple[str, bytes]:
        response = self.get(url)
        response.raise_for_status()

        content_disposition = response.headers["content-disposition"]
        filename = content_disposition.split("filename=")[1].strip('"')

        return filename, response.content

    @staticmethod
    def save_igc_file(filename: str, content: bytes) -> None:
        with open(filename, "wb") as f:
            f.write(content)

    def scrape_competition(self, competition_url: str) -> None:
        print(f"Starting scrape of competition: {competition_url}")
        task_days = self.get_task_days(competition_url)
        print(f"Found {len(task_days)} task days")
        for day_url in task_days:
            print(f"\nProcessing task day: {day_url}")
            igc_data = self.get_pilot_igc_links(day_url)
            for igc_url, start_time, finish_time, contestant, points in igc_data:
                if igc_url:
                    print(f"Downloading: {igc_url}")
                    filename, content = self.download_igc_data(igc_url)
                    content = self.append_times_to_igc(
                        content, start_time, finish_time, contestant, points
                    )
                    self.save_igc_file(filename, content)
                    print(f"Appended times: {start_time} -> {finish_time}")
        print("\nScraping complete.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("competition_url")
    args = parser.parse_args()
    scraper = SoaringSpotScraper()
    scraper.scrape_competition(args.competition_url)
    return 0


if __name__ == "__main__":
    exit(main())
