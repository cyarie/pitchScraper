"""
A Chris Yarie script for formatting the pitch/atbat CSV files and loading them into the database. Uses argparse to
set two modes, one for the initial database build, and the second for the updates.
"""

import argparse
import psycopg2
import csv
import os
from collections import namedtuple


def db_connect():
    try:
        conn = psycopg2.connect("dbname='{0}' "
                                "user='{0}' "
                                "host='{1}' "
                                "password='{2}' "
                                "port=5432 ".format(os.getenv("PITCHES_DB_NAME"),
                                                    os.getenv("PITCHES_DB_HOST"),
                                                    os.getenv("PITCHES_DB_PW"),))
        return conn
    except psycopg2.OperationalError as e:
        print("Error connection to the database: ", e)


def build_db():
    games_dir = "/home/tweets-deploy/pitchScraper/game_logs"
    games_gen = os.walk(games_dir)

    # Let's make a named tuple to pair up our game logs with the directories they belong to!
    GameTuple = namedtuple("GamePair", "directory game_id")

    # Call next() on the generator here to iterate over the first walk() object, which isn't of much use to us.
    next(games_gen)

    # Now, we can call a while loop on the remaining iterators in the generator. The lists here contain the names of
    # the files we want to process
    while True:
        try:
            game_iterator = next(games_gen)
            log_dir = game_iterator[0]
            game_list = game_iterator[2]
            for game in game_list:
                print("Processing {}".format(game))
                game_tuple = GameTuple(directory=log_dir, game_id=game)
                log_path = os.path.join(game_tuple.directory, game_tuple.game_id)
                with open(log_path, "r", newline="") as log_input:
                    print("Opening {}".format(log_path))
                    db_conn = db_connect()
                    cursor = db_conn.cursor()
                    log_reader = csv.DictReader(log_input)
                    for row in log_reader:
                        print(row)
                        if game.split("_")[len(game.split("_")) - 1].split(".")[0] == "atbats":
                            print("Doing atbats")
                            fields = log_reader.fieldnames
                            data_list = list()
                            for field in fields:
                                data_list.append(row[field].replace('"', ''))
                            print(data_list)
                            data_list.append(game.split(".")[0].replace("_atbats", ""))
                            print(data_list)
                            cursor.execute("""
                            INSERT INTO pitch_data.at_bats (ab_id, batter, pitcher, ab_des, game_id)
                                VALUES (%s, %s, %s, %s, %s);""", data_list)
                            print("Loaded at-bat")
                            db_conn.commit()
                        elif game.split("_")[len(game.split("_")) - 1].split(".")[0] == "pitches":
                            print("Doing pitches")
                            fields = log_reader.fieldnames
                            data_list = list()
                            for field in fields:
                                data_list.append(row[field].replace('"', ''))
                            for x in range(0, len(data_list)):
                                if data_list[x] == '':
                                    data_list[x] = None
                            print(data_list)
                            cursor.execute("""
                            INSERT INTO pitch_data.pitches (end_speed, pfx_x, px, sz_bot, ay, vy0, break_angle,
                                z0, ax, y, type, sv_id, spin_rate, mt, type_confidence, y0, des, vx0, break_y,
                                az, sz_top, ab_id, spin_dir, zone, start_speed, pz, tfs, x0, vz0, tfs_zulu,
                                event_num, break_length, play_guid, x, cc, nasty, id, pitch_type, pfx_z,
                                game_id) VALUES
                                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s);""", data_list)
                            print("Loaded pitch")
                            db_conn.commit()
                    db_conn.close()
                print("Finished {}".format(game))

        except StopIteration:
            print("Finished iterating.")
            break


def initial_build():
    pass


def update_db():
    pass


def main():
    """
    parser = argparse.ArgumentParser(description="A script for updating the PitchDB")
    parser.add_argument("--build", help="initial or update")
    parser.add_argument("--path", help="Path where all the game logs exist")
    args = parser.parse_args()

    if args.build and args.path:
        if args.build.lower() == "initial":
            print("Initial build")
            initial_build()
        elif args.build.lower() == "update":
            print("Updating the database...")
            update_db()
        else:
            print(parser.print_help())
    else:
        parser.print_help()
    """
    build_db()


if __name__ == "__main__":
    main()
