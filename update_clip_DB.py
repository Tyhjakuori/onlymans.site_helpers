#!/usr/bin/python3
#-*- coding: utf-8 -*-
import sys
import time
import json
import mariadb
import logging
import requests
import configparser
from auth_class import handle_auth
from datetime import datetime
from datetime import timedelta
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


def create_connection_pool(config):
    """ Connect to MariaDB pool or if that fails use regular connection """
    try:
        pool = mariadb.ConnectionPool(pool_name="clippool", pool_size=20, host=config['DB']['HOST'], user=config['DB']['USER'], password=config['DB']['PASS'], database=config['DB']['DB'])
        conn = pool.get_connection()
    except mariadb.PoolError as e:
        logging.warn("Error opening connection from pool %s", e)
        conn = mariadb.connect(
            host=config['DB']['HOST'],
            user=config['DB']['USER'],
            password=config['DB']['PASS'],
            database=config['DB']['DB']
        )
    return conn

def get_user_ids(config, users):
    user_ids = []
    for user in users:
        try:
            url = f'https://api.twitch.tv/helix/users?login={user}'
            response = requests.get(url, headers = {"Client-Id": config['DEFAULT']['client_id'],
                                                "Authorization": config['DEFAULT']['authorization']})
            if response.status_code != 200:
                logging.warn("Got response other than 200 from user id search: %i", response.status_code)
            else :
                logging.info("Got following response: %i", response.status_code)
                data = response.json()
                logging.info("Found %s which user id is %s", user, data['data'][0]['id'])
                user_ids.append(data['data'][0]['id'])
        except Exception as err:
            logging.error("get_user_ids function error: %s", err)
    return user_ids


logging.basicConfig(filename='update_clip_DB.log', encoding='utf-8', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y, %H:%M:%S')

handle_auth.test_auth_expiration()
config = configparser.ConfigParser()
config.read('.cfg.ini')
conn = create_connection_pool(config)
cur = conn.cursor()
#cur.execute("SELECT broadcaster FROM clips GROUP BY broadcaster HAVING COUNT(broadcaster) > 1 ORDER BY broadcaster ASC")
#users = cur.fetchall()
users_clip = ['Trellionspiers', 'NikosStudios', 'TheWombatOfDoom', 'MegaKirby', 'Z_Mukamuk']
logging.info("Found following broadcasters in clips: %s", users_clip)
gotten_user_ids = get_user_ids(config, users_clip)
logging.info(gotten_user_ids)
currentDateTime = datetime.now() + timedelta(days=-1)
date = currentDateTime.date()
year = date.strftime("%Y-%m-%d")

# Implement retry policy that will retry connection 5 times
# each time exponentially increasing wait time by 3
retry_strategy = Retry(
    total=5,
    backoff_factor=3,
    status_forcelist=[500, 502, 503, 504]
)

# Use Retry class
adapter = HTTPAdapter(max_retries=retry_strategy)

# Use requests session so we save bandwith and time when we don't have to create new request base everytime
session = requests.Session()
session.mount('https://', adapter)

# The absolute amount it will wait until giving up (60 seconds) and raising timeout error
absolute_backdown = 60

start_dates = ["2016-01-01T00:00:00Z", "2017-01-01T00:00:00Z", "2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z", "2020-01-01T00:00:00Z", "2021-01-01T00:00:00Z", "2022-01-01T00:00:00Z", "2023-01-01T00:00:00Z", "2024-01-01T00:00:00Z"]
end_dates = ["2016-12-31T23:59:00Z", "2017-12-31T23:59:00Z", "2018-12-31T23:59:00Z", "2019-12-31T23:59:00Z", "2020-12-31T23:59:00Z", "2021-12-31T23:59:00Z", "2022-12-31T23:59:00Z", "2023-12-31T23:59:00Z", f"{year}T23:59:00Z"]

# Get all clips
for broadcaster_id in gotten_user_ids:
    for start_day, end_day in zip(start_dates, end_dates):
        url1 = f"https://api.twitch.tv/helix/clips?broadcaster_id={broadcaster_id}&started_at={start_day}&ended_at={end_day}"
        logging.info("Opening url: %s", url1)
        print(url1)
        try:
            response = session.get(url1, timeout=absolute_backdown, headers = {"Client-Id": config['DEFAULT']['client_id'],
                "Authorization": config['DEFAULT']['authorization']})
        except requests.exceptions.Timeout as err:
            logging.warning("Connection timed out on first clips request: %s", err)
        print(response.status_code)
        logging.info("Got reponse: %i from request without cursor (first request)", response.status_code)
        data = response.json()
        for value in data['data']:
            date1 = datetime.strptime(value['created_at'], '%Y-%m-%dT%H:%M:%SZ')
            today_is = datetime.now()
            cur.execute("SELECT MAX(id) FROM clips")
            (table_id,) = cur.fetchone()
            table_id += 1
            parsed = today_is.strftime("%Y-%m-%d %H:%M:%S")
            cur.execute("INSERT INTO clips (id, name, embed_url, broadcaster, creator_name, game_id, title, view_count, created_at, duration, added, available_twitch) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE view_count=VALUES(view_count), edited=VALUES(added)", (table_id, value['id'], value['embed_url'], value['broadcaster_name'], value['creator_name'], value['game_id'], value['title'], value['view_count'], date1, value['duration'], parsed, 1))
            conn.commit()
        print(data)
        #print(data['pagination']['cursor'])
        cursor = None
        timeout_length = 5
        if 'cursor' in data['pagination']:
            cursor = data['pagination']['cursor']
            while cursor:
                try:
                    url2 = f"https://api.twitch.tv/helix/clips?broadcaster_id={broadcaster_id}&started_at={start_day}&ended_at={end_day}&after={cursor}"
                    response1 = session.get(url2, timeout=absolute_backdown, headers = {"Client-Id": config['DEFAULT']['client_id'],
                        "Authorization": config['DEFAULT']['authorization']})
                    logging.info("Got reponse: %i from request with cursor", response.status_code)
                    data1 = response1.json()
                    for value in data1['data']:
                        date1 = datetime.strptime(value['created_at'], '%Y-%m-%dT%H:%M:%SZ')
                        today_is = datetime.now()
                        cur.execute("SELECT MAX(id) FROM clips")
                        (table_id,) = cur.fetchone()
                        table_id += 1
                        parsed = today_is.strftime("%Y-%m-%d %H:%M:%S")
                        cur.execute("INSERT INTO clips (id, name, embed_url, broadcaster, creator_name, game_id, title, view_count, created_at, duration, added, available_twitch) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE view_count=VALUES(view_count), edited=VALUES(added)", (table_id, value['id'], value['embed_url'], value['broadcaster_name'], value['creator_name'], value['game_id'], value['title'], value['view_count'], date1, value['duration'], parsed, 1))
                        conn.commit()

                    if 'cursor' in data1['pagination']:
                        cursor = data1['pagination']['cursor']
                        time.sleep(5)
                    else:
                        cursor = None
                except requests.exceptions.Timeout as err:
                    logging.warning("Connection timed out on cursor clips request: %s... lengthening timeout_length", err)

today_is = datetime.now()
parsed = today_is.strftime("%Y-%m-%d %H:%M:%S")
logging.info(parsed)
cur.execute("INSERT INTO db_updated (id, table_name, updated) VALUES (?,?,?)", (None, "clips", parsed,))
conn.commit()

# Get all highlights
for broadcaster_id in gotten_user_ids:
    url1 = f"https://api.twitch.tv/helix/videos?user_id={broadcaster_id}&type=highlight&first=100"
    logging.info("Opening url: %s", url1)
    try:
        response = session.get(url1, timeout=absolute_backdown ,headers = {"Client-Id": config['DEFAULT']['client_id'],
            "Authorization": config['DEFAULT']['authorization']})
    except requests.exceptions.Timeout as err:
        logging.warning("Connection timed out on first highlights request: %s", err)
    logging.info("Got reponse: %i from request without cursor (first request)", response.status_code)
    data = response.json()
    for value in data['data']:
        date1 = datetime.strptime(value['created_at'], '%Y-%m-%dT%H:%M:%SZ')
        url1 = value['url'].split("videos/")[1]
        cur.execute("SELECT MAX(id) FROM highlights")
        (high_id,) = cur.fetchone()
        high_id += 1
        today_is = datetime.now()
        parsed = today_is.strftime("%Y-%m-%d %H:%M:%S")
        cur.execute("INSERT INTO highlights (id, title, url, user_name, description, thumbnail_url, viewable, view_count, created_at, duration, added, available_twitch) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE view_count=VALUES(view_count), edited=VALUES(added)", (high_id, value['title'], url1, value['user_name'], value['description'], value['thumbnail_url'], value['viewable'], value['view_count'], date1, value['duration'], parsed, 1,))
        conn.commit()
    #print(data['pagination']['cursor'])
    cursor = None
    timeout_length = 5
    if 'cursor' in data['pagination']:
        cursor = data['pagination']['cursor']
        while cursor:
            try:
                url2 = f"https://api.twitch.tv/helix/videos?user_id={broadcaster_id}&type=highlight&first=100&after={cursor}"
                response1 = session.get(url2, timeout=absolute_backdown, headers = {"Client-Id": config['DEFAULT']['client_id'],
                    "Authorization": config['DEFAULT']['authorization']})
                logging.info("Got reponse: %i from request with cursor", response.status_code)
                data1 = response1.json()
                print(data1)
                for value in data1['data']:
                    date1 = datetime.strptime(value['created_at'], '%Y-%m-%dT%H:%M:%SZ')
                    cur.execute("SELECT MAX(id) FROM highlights")
                    (high_id,) = cur.fetchone()
                    high_id += 1
                    url1 = value['url'].split("videos/")[1]
                    today_is = datetime.now()
                    parsed = today_is.strftime("%Y-%m-%d %H:%M:%S")
                    cur.execute("INSERT INTO highlights (id, title, url, user_name, description, thumbnail_url, viewable, view_count, created_at, duration, added, available_twitch) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE view_count=VALUES(view_count), edited=VALUES(added)", (high_id, value['title'], url1, value['user_name'], value['description'], value['thumbnail_url'], value['viewable'], value['view_count'], date1, value['duration'], parsed, 1,))
                    conn.commit()
                if 'cursor' in data1['pagination']:
                    cursor = data1['pagination']['cursor']
                    time.sleep(5)
                else:
                    cursor = None
            except requests.exceptions.Timeout as err:
                logging.warning("Connection timed out on cursor clips request: %s... lengthening timeout_length", err)

today_is = datetime.now()
parsed = today_is.strftime("%Y-%m-%d %H:%M:%S")
cur.execute("INSERT INTO db_updated (id, table_name, updated) VALUES (?,?,?)", (None, "highlights", parsed,))
conn.commit()

# Join game_ids as game names found in game_ids table
cur.execute("UPDATE IGNORE clips JOIN game_ids ON clips.game_id = game_ids.id SET clips.game_name = game_ids.name")
conn.commit()
