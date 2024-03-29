#!/usr/bin/python3
# -*- coding: utf-8 -*-
import sys
import csv
import logging
import mariadb
import datetime
import configparser


def create_connection_pool(config):
    """Connect to MariaDB pool or if that fails use regular connection"""
    try:
        pool = mariadb.ConnectionPool(
            pool_name="clippool",
            pool_size=20,
            host=config["DB"]["HOST"],
            user=config["DB"]["USER"],
            password=config["DB"]["PASS"],
            database=config["DB"]["DB"],
        )
        conn = pool.get_connection()
    except mariadb.PoolError as e:
        logging.warn(f"Error opening connection from pool {e}")
        conn = mariadb.connect(
            host=config["DB"]["HOST"],
            user=config["DB"]["USER"],
            password=config["DB"]["PASS"],
            database=config["DB"]["DB"],
        )
    return conn


logging.basicConfig(
    filename="update_clip_DB.log",
    encoding="utf-8",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%d/%m/%Y, %H:%M:%S",
)

config = configparser.ConfigParser()
config.read(".cfg.ini")
conn = create_connection_pool(config)
cur = conn.cursor()
filename = "update_highlights.csv"
with open(filename, "r", encoding="utf-8") as csvfile:
    reader = csv.reader(csvfile, delimiter=",")
    try:
        next(reader, None)
        output = list(reader)
    except csv.Error as e:
        sys.exit("file {}, line {}: {}".format(filename, reader.line_num, e))

values = [tuple(i) for i in output]
cur.executemany(
    "UPDATE highlights SET game_name=(?), edited=(?) WHERE url=(?)", (values)
)
conn.commit()

today_is = datetime.datetime.now()
parsed = today_is.strftime("%Y-%m-%d %H:%M:%S")
cur.execute(
    "INSERT INTO db_updated (id, table_name, updated) VALUES (?,?,?)",
    (
        None,
        "highlights",
        parsed,
    ),
)
conn.commit()
