#!/usr/bin/python
# -*- coding: iso-8859-15 -*-
import sys
import time
import json
import mariadb
import logging
import requests
import datetime
import configparser


def create_connection_pool(config):
    """ Connect to MariaDB pool or if that fails use regular connection """
    try:
        pool = mariadb.ConnectionPool(pool_name="clippool", pool_size=20, host=config['DB']['HOST'], user=config['DB']['USER'], password=config['DB']['PASS'], database=config['DB']['DB'])
        conn = pool.get_connection()
    except mariadb.PoolError as e:
        logging.warn(f"Error opening connection from pool {e}. Using regular connection...")
        conn = mariadb.connect(
            host=config['DB']['HOST'],
            user=config['DB']['USER'],
            password=config['DB']['PASS'],
            database=config['DB']['DB']
        )
    return conn

def test_auth_expiration():
    """ Test if access token expired, refresh if needed """
    logging.info("Testing is access token expired...")
    config = configparser.ConfigParser()
    config.read('.cfg.ini')
    test_url = "https://api.twitch.tv/helix/streams"
    response = requests.get(test_url, headers = {"Client-Id": config['DEFAULT']['client_id'],
                                            "Authorization": config['DEFAULT']['authorization']})
    logging.info("Got {} code from the request".format(response.status_code))
    if response.status_code != 200:
        refresh_token(config)
    else:
        return

def refresh_token(config):
    """ Refesh your access token via Twitch api request """
    refresh_url = "https://id.twitch.tv/oauth2/token"
    credentials_obj = {"client_id": config['DEFAULT']['client_id'], "client_secret": config['DEFAULT']['client_secret'], "grant_type": config['DEFAULT']['grant_type']}
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    response_rersh = requests.post(refresh_url, headers=headers, data=credentials_obj)
    if response_rersh.status_code != 200:
        logging.error(f"There was an error while trying to refresh access token: {response_rersh}")
        sys.exit()
    else:
        refresh_conf(config, response_rersh.json())

def refresh_conf(config, new_vals):
    """ Update config file with new access token """
    get_section = config["DEFAULT"]
    type_capitalize = new_vals["token_type"].capitalize()
    get_section["authorization"] = "{} {}".format(type_capitalize, new_vals["access_token"])
    if not config.has_section("UPDATED"):
        config.add_section("UPDATED")
    today = datetime.datetime.now()
    config.set("UPDATED", "; updated on {} with {}".format(today, new_vals), '')
    with open('.cfg.ini', 'w') as configfile:
        config.write(configfile)
    return

def get_user_ids(config, users):
	""" Get user ids from Twitch api """
    user_ids = []
    for user in users:
        try:
            #url = 'https://api.twitch.tv/helix/users?login={}'.format(user[0])
            url = 'https://api.twitch.tv/helix/users?login={}'.format(user)
            response = requests.get(url, headers = {"Client-Id": config['DEFAULT']['client_id'],
                                                "Authorization": config['DEFAULT']['authorization']})
            if response.status_code != 200:
                logging.warn(f"Got response other than 200 from user id search: {response.status_code}")
            else :
                logging.info(f"Got following response: {response.status_code}")
                data = response.json()
                #logging.info(f"Found {user[0]} which user id is {data['data'][0]['id']}")
                logging.info(f"Found {user} which user id is {data['data'][0]['id']}")
                user_ids.append(data['data'][0]['id'])
        except Exception as err:
            logging.error(f"get_user_ids function error: {err}")
    return user_ids


logging.basicConfig(filename='update_clip_DB.log', encoding='utf-8', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y, %H:%M:%S')

test_auth_expiration()
config = configparser.ConfigParser()
config.read('.cfg.ini')
conn = create_connection_pool(config)
cur = conn.cursor()
#cur.execute("SELECT broadcaster FROM clips GROUP BY broadcaster HAVING COUNT(broadcaster) > 1 ORDER BY broadcaster ASC")
#users = cur.fetchall()
users_clip = ['Trellionspiers', 'NikosStudios', 'TheWombatOfDoom', 'MegaKirby']
logging.info(f"Found following broadcasters in clips: {users_clip}")
gotten_user_ids = get_user_ids(config, users_clip)
logging.info(gotten_user_ids)

start_dates = ["2016-01-01T00:00:00Z", "2017-01-01T00:00:00Z", "2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z", "2020-01-01T00:00:00Z", "2021-01-01T00:00:00Z", "2022-01-01T00:00:00Z", "2023-01-01T00:00:00Z"]
end_dates = ["2016-12-31T23:59:00Z", "2017-12-31T23:59:00Z", "2018-12-31T23:59:00Z", "2019-12-31T23:59:00Z", "2020-12-31T23:59:00Z", "2021-12-31T23:59:00Z", "2022-12-31T23:59:00Z", "2023-12-31T23:59:00Z"]
for broadcaster_id in gotten_user_ids:
    for start_day, end_day in zip(start_dates, end_dates):
        url1 = "https://api.twitch.tv/helix/clips?broadcaster_id={}&started_at={}&ended_at={}".format(broadcaster_id, start_day, end_day)
        logging.info(f"Opening url: {url1}")
        print(url1)
        response = requests.get(url1, headers = {"Client-Id": config['DEFAULT']['client_id'],
            "Authorization": config['DEFAULT']['authorization']})
        print(response.status_code)
        logging.info(f"Got reponse: {response.status_code} from request without cursor (first request)")
        data = response.json()
        for value in data['data']:
            date1 = datetime.datetime.strptime(value['created_at'], '%Y-%m-%dT%H:%M:%SZ')
            today_is = datetime.datetime.now()
            parsed = today_is.strftime("%Y-%m-%d %H:%M:%S")
            cur.execute("INSERT INTO clips (id, name, embed_url, broadcaster, creator_name, game_id, title, view_count, created_at, duration, added, available_twitch) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE view_count=VALUES(view_count), edited=VALUES(added)", (None, value['id'], value['embed_url'], value['broadcaster_name'], value['creator_name'], value['game_id'], value['title'], value['view_count'], date1, value['duration'], parsed, 1))
            conn.commit()
        print(data)
        #print(data['pagination']['cursor'])
        cursor = None
        timeout_length = 2
        if 'cursor' in data['pagination']:
            cursor = data['pagination']['cursor']
            while cursor:
                try:
                    url2 = "https://api.twitch.tv/helix/clips?broadcaster_id={}&started_at={}&ended_at={}&after={}".format(broadcaster_id, start_day, end_day, cursor)
                    response1 = requests.get(url2, timeout=timeout_length, headers = {"Client-Id": config['DEFAULT']['client_id'],
                        "Authorization": config['DEFAULT']['authorization']})
                    logging.info(f"Got reponse: {response.status_code} from request with cursor")
                    data1 = response1.json()
                    for value in data1['data']:
                        date1 = datetime.datetime.strptime(value['created_at'], '%Y-%m-%dT%H:%M:%SZ')
                        today_is = datetime.datetime.now()
                        parsed = today_is.strftime("%Y-%m-%d %H:%M:%S")
                        cur.execute("INSERT INTO clips (id, name, embed_url, broadcaster, creator_name, game_id, title, view_count, created_at, duration, added, available_twitch) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE view_count=VALUES(view_count), edited=VALUES(added)", (None, value['id'], value['embed_url'], value['broadcaster_name'], value['creator_name'], value['game_id'], value['title'], value['view_count'], date1, value['duration'], parsed, 1))
                        conn.commit()

                    if 'cursor' in data1['pagination']:
                        cursor = data1['pagination']['cursor']
                        time.sleep(5) # muuta sekunttiin kun request limit on käytössä
                    else:
                        cursor = None
                except requests.exceptions.ConnectTimeout:
                    timeout_length **= 2

today_is = datetime.datetime.now()
parsed = today_is.strftime("%Y-%m-%d %H:%M:%S")
logging.info(parsed)
cur.execute("INSERT INTO db_updated (id, table_name, updated) VALUES (?,?,?)", (None, "clips", parsed,))
conn.commit()

for broadcaster_id in gotten_user_ids:
    url1 = "https://api.twitch.tv/helix/videos?user_id={}&type=highlight&first=100".format(broadcaster_id)
    logging.info(f"Opening url: {url1}")
    response = requests.get(url1, headers = {"Client-Id": config['DEFAULT']['client_id'],
        "Authorization": config['DEFAULT']['authorization']})
    logging.info(f"Got reponse: {response.status_code} from request without cursor (first request)")
    data = response.json()
    for value in data['data']:
        date1 = datetime.datetime.strptime(value['created_at'], '%Y-%m-%dT%H:%M:%SZ')
        url1 = value['url'].split("videos/")[1]
        today_is = datetime.datetime.now()
        parsed = today_is.strftime("%Y-%m-%d %H:%M:%S")
        cur.execute("INSERT INTO highlights (id, title, url, user_name, description, thumbnail_url, viewable, view_count, created_at, duration, added, available_twitch) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE view_count=VALUES(view_count), edited=VALUES(added)", (None, value['title'], url1, value['user_name'], value['description'], value['thumbnail_url'], value['viewable'], value['view_count'], date1, value['duration'], parsed, 1,))
        conn.commit()
    #print(data['pagination']['cursor'])
    cursor = None
    timeout_length = 2
    if 'cursor' in data['pagination']:
        cursor = data['pagination']['cursor']
        while cursor:
            try:
                url2 = "https://api.twitch.tv/helix/videos?user_id={}&type=highlight&first=100&after={}".format(broadcaster_id, cursor)
                response1 = requests.get(url2, timeout=timeout_length, headers = {"Client-Id": config['DEFAULT']['client_id'],
                    "Authorization": config['DEFAULT']['authorization']})
                logging.info(f"Got reponse: {response.status_code} from request with cursor")
                data1 = response1.json()
                print(data1)
                for value in data1['data']:
                    date1 = datetime.datetime.strptime(value['created_at'], '%Y-%m-%dT%H:%M:%SZ')
                    url1 = value['url'].split("videos/")[1]
                    today_is = datetime.datetime.now()
                    parsed = today_is.strftime("%Y-%m-%d %H:%M:%S")
                    cur.execute("INSERT INTO highlights (id, title, url, user_name, description, thumbnail_url, viewable, view_count, created_at, duration, added, available_twitch) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE view_count=VALUES(view_count), edited=VALUES(added)", (None, value['title'], url1, value['user_name'], value['description'], value['thumbnail_url'], value['viewable'], value['view_count'], date1, value['duration'], parsed, 1,))
                    conn.commit()
                if 'cursor' in data1['pagination']:
                    cursor = data1['pagination']['cursor']
                    time.sleep(5) # muuta sekunttiin kun request limit on käytössä
                else:
                    cursor = None
            except requests.exceptions.ConnectTimeout:
                timeout_length **= 2

today_is = datetime.datetime.now()
parsed = today_is.strftime("%Y-%m-%d %H:%M:%S")
cur.execute("INSERT INTO db_updated (id, table_name, updated) VALUES (?,?,?)", (None, "highlights", parsed,))
conn.commit()
