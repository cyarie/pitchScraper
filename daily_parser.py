"""
This is a Chris Yarie script to scrape all the MLB Gameday data from their website, and to then turn that XML data
into CSVs. Script will run around 10 AM each day, and scrape Gameday (mostly PitchFX right now, but StatCast soon)
data for further processing and loading.
"""

import requests
import collections
import re
import csv
import os

from lxml import etree, html
from datetime import date, timedelta


def scrape_page(scrape_date):
    game_urls = []
    # I do not think named tuples should be named in lowercase -- CamelCase is okay here.
    GameTuple = collections.namedtuple("GameRecord", "name link")
    base_url = "http://gd2.mlb.com/components/game/mlb/"
    daily_url = "{0}year_{1}/month_{2}/day_{3}/".format(base_url,
                                                        scrape_date.year,
                                                        scrape_date.strftime("%m"),
                                                        scrape_date.strftime("%d"))
    day_page = requests.get(daily_url)
    day_tree = html.fromstring(day_page.text)
    link_re = re.compile(r"gid_\d{4}_\d{2}_\d{2}_(\w+?_){2}\d/")
    for link in day_tree.iterlinks():
        gid_match = link[2]
        if link_re.match(gid_match):
            game_data = GameTuple(name=gid_match.rstrip("/"),
                                  link="{0}{1}inning/inning_all.xml".format(daily_url, gid_match))
            game_urls.append(game_data)
    return game_urls


def parse_gd_xml(game_tuple, scrape_date):
    r = requests.get(game_tuple.link)
    print(r.status_code)
    if r.status_code != 404:
        today_date = scrape_date.strftime("%Y_%m_%d")
        xml_str = r.text.encode("utf-8", "replace")

        game = etree.fromstring(xml_str)
        path = os.path.join(os.getcwd(), "{0}_games".format(today_date))

        if not os.path.isdir(path):
            os.mkdir(path)

        with open(os.path.join(path, "{0}_atbats.csv".format(game_tuple.name)), "w", newline="") as csv_file:
            ab_headers = ['ab_id', 'batter', 'pitcher', 'ab_des', 'game_id']
            writer = csv.DictWriter(csv_file, fieldnames=ab_headers)
            writer.writeheader()
            for inning in game:
                for half in inning:
                    for atbat in half:
                        if str(atbat.tag) == "atbat":
                            ab_dict = collections.defaultdict()
                            for key in atbat.keys():
                                ab_dict[key] = atbat.get(key)
                            writer.writerow({"ab_id": int(ab_dict["num"]),
                                             "batter": int(ab_dict["batter"]),
                                             "pitcher": int(ab_dict["pitcher"]),
                                             "ab_des": str(ab_dict["des"]),
                                             "game_id": str(game_tuple.name)})

        with open(os.path.join(path, "{0}_pitches.csv".format(game_tuple.name)), "w", newline="") as csv_file:
            pitch_headers = ['end_speed', 'pfx_x', 'px', 'sz_bot', 'ay', 'vy0', 'break_angle', 'z0', 'ax', 'y',
                             'type', 'sv_id', 'spin_rate', 'mt', 'type_confidence', 'y0', 'des', 'vx0', 'break_y',
                             'az', 'sz_top', 'ab_id', 'spin_dir', 'zone', 'start_speed', 'pz', 'tfs', 'x0', 'vz0',
                             'tfs_zulu', 'event_num', 'break_length', 'play_guid', 'x', 'cc', 'nasty', 'id',
                             'pitch_type', 'pfx_z', 'game_id']
            writer = csv.DictWriter(csv_file, fieldnames=pitch_headers)
            writer.writeheader()
            for inning in game:
                for half in inning:
                    for atbat in half:
                        ab_id = atbat.get("num")
                        for pitch in atbat:
                            if str(pitch.tag) == "pitch":
                                pitch_dict = collections.defaultdict()
                                for key in pitch.keys():
                                    pitch_dict[key] = pitch.get(key)
                                try:
                                    writer.writerow({"ab_id": int(ab_id),
                                                     "play_guid": pitch_dict["play_guid"],
                                                     "start_speed": float(pitch_dict["start_speed"]),
                                                     "end_speed": float(pitch_dict["end_speed"]),
                                                     "pfx_x": float(pitch_dict["pfx_x"]),
                                                     "px": float(pitch_dict["px"]),
                                                     "sz_bot": float(pitch_dict["sz_bot"]),
                                                     "ay": float(pitch_dict["ay"]),
                                                     "vy0": float(pitch_dict["vy0"]),
                                                     "break_angle": float(pitch_dict["break_angle"]),
                                                     "z0": float(pitch_dict["z0"]),
                                                     "ax": float(pitch_dict["ax"]),
                                                     "y": float(pitch_dict["y"]),
                                                     "type": pitch_dict["type"],
                                                     "sv_id": pitch_dict["sv_id"],
                                                     "spin_rate": float(pitch_dict["spin_rate"]),
                                                     "mt": pitch_dict["mt"],
                                                     "type_confidence": float(pitch_dict["type_confidence"]),
                                                     "y0": float(pitch_dict["y0"]),
                                                     "des": pitch_dict["des"],
                                                     "vx0": float(pitch_dict["vx0"]),
                                                     "break_y": float(pitch_dict["break_y"]),
                                                     "az": float(pitch_dict["az"]),
                                                     "sz_top": float(pitch_dict["sz_top"]),
                                                     "spin_dir": float(pitch_dict["spin_dir"]),
                                                     "zone": int(pitch_dict["zone"]),
                                                     "pz": float(pitch_dict["pz"]),
                                                     "tfs": pitch_dict["tfs"],
                                                     "x0": float(pitch_dict["x0"]),
                                                     "tfs_zulu": pitch_dict["tfs_zulu"],
                                                     "event_num": pitch_dict["event_num"],
                                                     "break_length": float(pitch_dict["break_length"]),
                                                     "x": float(pitch_dict["x"]),
                                                     "cc": pitch_dict["cc"],
                                                     "nasty": int(pitch_dict["nasty"]),
                                                     "id": int(pitch_dict["id"]),
                                                     "pitch_type": pitch_dict["pitch_type"],
                                                     "pfx_z": float(pitch_dict["pfx_z"]),
                                                     "game_id": str(game_tuple.name)})
                                except KeyError:
                                    pass
                            else:
                                pass


def main():
    for day in range(1, 2062):
        game_tuples = scrape_page(date.today()-timedelta(days=day))
        for game in game_tuples:
            parse_gd_xml(game, date.today()-timedelta(days=day))

if __name__ == "__main__":
    main()