import sys
import configparser
import datetime
import requests


class handle_auth:
    def test_auth_expiration():
        """Test if access token expired, refresh if needed"""
        print("Testing is access token expired...")
        config = configparser.ConfigParser()
        config.read(".cfg.ini")
        test_url = "https://api.twitch.tv/helix/streams"
        response = requests.get(
            test_url,
            headers={
                "Client-Id": config["DEFAULT"]["client_id"],
                "Authorization": config["DEFAULT"]["authorization"],
            },
        )
        print(f"Got {response.status_code} code from the request")
        if response.status_code != 200:
            handle_auth.refresh_token(config)
        else:
            return

    def refresh_token(config):
        """Refesh your access token via Twitch api request"""
        refresh_url = "https://id.twitch.tv/oauth2/token"
        credentials_obj = {
            "client_id": config["DEFAULT"]["client_id"],
            "client_secret": config["DEFAULT"]["client_secret"],
            "grant_type": config["DEFAULT"]["grant_type"],
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response_rersh = requests.post(
            refresh_url, headers=headers, data=credentials_obj
        )
        if response_rersh.status_code != 200:
            print(
                f"There was an error while trying to refresh access token: {response_rersh}"
            )
            sys.exit()
        else:
            handle_auth.refresh_conf(config, response_rersh.json())

    def refresh_conf(config, new_vals):
        """Update config file with new access token"""
        get_section = config["DEFAULT"]
        type_capitalize = new_vals["token_type"].capitalize()
        get_section["authorization"] = "{} {}".format(
            type_capitalize, new_vals["access_token"]
        )
        if not config.has_section("UPDATED"):
            config.add_section("UPDATED")
        today = datetime.datetime.now()
        config.set("UPDATED", f"; updated on {today} with {new_vals}", "")
        with open(".cfg.ini", "w") as configfile:
            config.write(configfile)
        return
