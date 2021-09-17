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
    CHART_KEYS = [
        "data-chart-code",
        "data-chart-name",
        "data-chart-date",
        "data-chart-slug",
    ]
    POSITION_ARTIST_KEYS = [
        # definitely interesting
        "artist_name",
        "artist_slug",
        "artist_url",
        # maybe interesting
        "artist_id",
        "artist_brightcove_id",
        "artist_content_url",
        "artist_vevo_id",
    ]
    POSITION_SONG_KEYS = [
        # definitely interesting
        "title",
        # maybe interesting
        "bdssongid",
        "title_brightcove_id",
        "title_content_url",
        "title_id",
        "title_vevo_id",
    ]
    POSITION_KEYS = [
        # definitely interesting
        "rank",
        # maybe interesting
        "content_url",
    ]
    POSITION_HISTORY_KEYS = [
        # definitely interesting
        "peak_date",
        "peak_rank",
        # maybe interesting
        "first_pos_weeks",
    ]

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
        # This parsing works for hot-100. Haven't checked for other charts yet.
        soup = BeautifulSoup(html, features="html.parser")
        chart_node = soup.find(id="charts")
        if not chart_node:
            return
        chart = self.parse_chart_metadata(chart_node)
        position_data = json.loads(chart_node["data-charts"])
        positions = [self.parse_position(position) for position in position_data]
        return {
            "chart": chart,
            "positions": positions,
        }

    def parse_chart_metadata(self, chart_node):
        return {
            strip_prefix(key, "data-chart-"): chart_node.get(key)
            for key in self.CHART_KEYS
        }

    def parse_position(self, position_data):
        artist = {
            strip_prefix(key, "artist_"): position_data.get(key)
            for key in self.POSITION_ARTIST_KEYS
        }
        song = {
            strip_prefix(key, "title_"): position_data.get(key)
            for key in self.POSITION_SONG_KEYS
        }
        position = {key: position_data.get(key) for key in self.POSITION_KEYS}
        history_data = position_data.get("history", {})
        history = {key: history_data.get(key) for key in self.POSITION_HISTORY_KEYS}
        position.update(history)
        return {
            "artist": artist,
            "song": song,
            "position": position,
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
        peak = "*" if position["position"]["peak_date"] == chart_date else ""
        name = f"{position['song']['title']} - {position['artist']['name']}"
        print(f"{position['position']['rank']:2d} {peak:1s} {name:50s}")


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
