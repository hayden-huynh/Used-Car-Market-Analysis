from curl_cffi import requests
from dotenv import load_dotenv
import random
import os

load_dotenv(override=True)
proxies = {
    "http": os.getenv("STICKY_SMARTPROXY"),
    "https": os.getenv("STICKY_SMARTPROXY"),
}


def send_curl_request(url, useProxy=False):
    browser = random.choice(["chrome", "edge", "safari", "firefox"])
    resp = requests.get(
        url,
        impersonate=browser,
        proxies=proxies if useProxy else None,
        verify=False,
    )
    return resp


def get_curl_session():
    browser = random.choice(["chrome", "edge", "safari", "firefox"])
    session = requests.Session(
        impersonate=browser,
        proxies=proxies,
        verify=False,
    )
    return session
