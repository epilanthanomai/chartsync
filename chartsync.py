import argparse
import json
import os
import sys
from configparser import ConfigParser
from functools import partial
from pathlib import PosixPath
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from cachecontrol import CacheControl
from cachecontrol.caches import FileCache

ROOT_DIR = PosixPath("~/.chartsync").expanduser()


def strip_prefix(s, prefix):
    return s[len(prefix) :] if s.startswith(prefix) else s


class JsonCache:
    def __init__(self, root):
        self.root = root
        self._makedirs_flag = False

    def ensure_root(self):
        if not self._makedirs_flag:
            os.makedirs(self.root, exist_ok=True)
            self._makedirs_flag = True

    def get(self, key):
        path = self.root / key
        try:
            with open(path) as data_file:
                return self.unmarshal(data_file)
        except FileNotFoundError:
            return None

    def put(self, key, value):
        self.ensure_root()
        path = self.root / key
        with open(path, "w") as data_file:
            self.marshal(value, data_file)

    def unmarshal(self, data_file):
        return json.load(data_file)

    def marshal(self, value, data_file):
        json.dump(value, data_file)

    def make_auto_fetch(self, make_key, fetch):
        return partial(self.auto_fetch, make_key, fetch)

    def auto_fetch(self, make_key, fetch, *args, **kwargs):
        key = make_key(*args, **kwargs)
        value = self.get(key)
        if value is None:
            value = fetch(*args, **kwargs)
            self.put(key, value)
        return value


class Billboard:
    BASE_URL = "https://www.billboard.com/"

    def __init__(self, session):
        self.session = session
        self.chart_cache = JsonCache(ROOT_DIR / "billboard.com" / "charts")
        self.get_chart = self.chart_cache.make_auto_fetch(
            self.chart_key, self.request_chart
        )

    def chart_key(self, chart, date):
        return f"{chart}-{date}"

    def request_chart(self, chart, date):
        chart_url = self.chart_url(chart, date)
        response = self.session.get(chart_url)
        response.raise_for_status()
        result = self.parse_chart(response.text)
        assert result["chart"]["slug"] == chart
        assert result["chart"]["date"] == date
        assert result["positions"]
        return result

    def chart_url(self, chart, date):
        return urljoin(self.BASE_URL, f"charts/{chart}/{date}")

    def fetch_chart(self, chart_url):
        response = self.session.get(chart_url)
        response.raise_for_status()
        return response.text

    def parse_chart(self, html):
        soup = BeautifulSoup(html, features="html.parser")
        return {
            "chart": self.scrape_chart_data(soup),
            "positions": self.scrape_position_data(soup),
        }

    def scrape_chart_data(self, soup):
        schemaorg_objects = self.scrape_schemaorg_metadata(soup)
        chart_data = self.chart_data_from_schemaorg(schemaorg_objects)
        chart_data["date"] = self.scrape_chart_date(soup)
        return chart_data

    def scrape_schemaorg_metadata(self, soup):
        nodes = soup("script", type="application/ld+json")
        objects = [json.loads(node.text) for node in nodes]
        return [o for o in objects if o]

    def chart_data_from_schemaorg(self, schemaorg_objects):
        schemaorg_article = [o for o in schemaorg_objects if o["@type"] == "Article"][0]
        title = schemaorg_article["headline"]
        chart_url = schemaorg_article["mainEntityOfPage"]["@id"]
        _, _, chart_slug = chart_url.rstrip("/").rpartition("/")
        return {
            "name": schemaorg_article["headline"],
            "slug": chart_slug,
        }

    def scrape_chart_date(self, soup):
        picker_node = soup.find(id="chart-date-picker")
        return picker_node["data-date"]

    def scrape_position_data(self, soup):
        nodes = soup(class_="o-chart-results-list-row-container")
        return [self.position_data_from_node(n) for n in nodes]

    def position_data_from_node(self, node):
        row_node = node.find(class_="o-chart-results-list-row")
        row_item_nodes = row_node(class_="o-chart-results-list__item")

        rank_node = row_item_nodes[0].span
        title_node = row_item_nodes[3]
        previous_rank_node = row_item_nodes[6]
        peak_rank_node = row_item_nodes[7]
        chart_weeks_node = row_item_nodes[8]

        try:
            previous_rank = int(previous_rank_node.text)
        except ValueError:
            previous_rank = None

        return {
            "artist": {"name": title_node.span.text.strip()},
            "song": {"title": title_node.h3.text.strip()},
            "position": {
                "rank": int(rank_node.text),
                "peak_rank": int(peak_rank_node.text),
                "previous_rank": previous_rank,
                "chart_weeks": int(chart_weeks_node.text),
            },
        }


class Printer:
    POSITION_LIMIT = 10

    def print(self, s):
        print(s)

    def chart(self, chart):
        date = chart["chart"]["date"]
        self.print_chart_header(chart)
        for position in chart["positions"][: self.POSITION_LIMIT]:
            self.print_chart_position(position, chart_date=date)

    def print_chart_header(self, chart):
        self.print(f"{chart['chart']['name']} for {chart['chart']['date']}")

    def print_chart_position(self, position, chart_date):
        rank = position["position"]["rank"]
        previous_rank = position["position"]["previous_rank"]
        compare = self.compare_ranks(rank, previous_rank)
        move = {1: "^   ", 0: " -  ", -1: "  v ", None: "   *"}[compare]
        name = f"{position['song']['title']} - {position['artist']['name']}"
        print(f"{position['position']['rank']:2d} {move:4s} {name:50s}")

    def compare_ranks(self, current, previous):
        if previous is None:
            return None
        if current < previous:
            return 1
        if current > previous:
            return -1
        return 0


def load_config():
    path = ROOT_DIR / "chartsync.conf"
    config = ConfigParser()
    config.read(path)
    return config


def get_session():
    base_session = requests.session()
    base_session.headers.update({"User-agent": "chartsync/0.1"})
    cache_dir = ROOT_DIR / "web-cache"
    cache = FileCache(cache_dir)
    return CacheControl(base_session, cache=cache)


def get_argument_parser():
    parser = argparse.ArgumentParser()
    parser.set_defaults(subcommand=print_chart)
    subparsers = parser.add_subparsers(required=False)

    print_parser = subparsers.add_parser("print")
    print_parser.set_defaults(subcommand=print_chart)
    print_parser.add_argument("date", nargs="?")

    return parser


def print_chart(session, config, args):
    billboard = Billboard(session)
    section = config["billboard.com"]
    chart_slug = section["chart"]
    date = args.date or section["week"]
    chart = billboard.get_chart(chart_slug, date)
    printer = Printer()
    printer.chart(chart)


def main():
    parser = get_argument_parser()
    args = parser.parse_args(sys.argv[1:])

    config = load_config()
    session = get_session()

    args.subcommand(session, config, args)


if __name__ == "__main__":
    main()
