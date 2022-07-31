#!/usr/bin/env python3


import sys, os


# sys.path.append(os.environ["PYLIB"])
# sys.path.insert(0,os.path.join(os.environ["PROJECTS"], "20220312_baiduphoto"))
# from LogConfig import LogConfig
# LogConfig.set_logtoScreen("debug")

from pybaiduphoto import API


import logging
import argparse
import json
import browser_cookie3
import requests


from wsgidav import wsgidav_app
from wsgidav.wsgidav_app import WsgiDAVApp
from wsgidav.default_conf import DEFAULT_CONFIG
from cheroot import wsgi
from yikeProvider import baiduphoto as Provider


sysConfig_default = {
    "ALBUM_DELETE_WITHITEM": False,
    "ALBUM_ITEM_DELETE_WITH_ORIGIN": False,
    "ITEM_NUM_MAX_IN_DIR": 999,
    "ITEM_NUM_MAX_IN_ALBUM": 999,
    "DELIMITER": "@",
}


def dumpCookies(cj, filePath):
    cookies = requests.utils.dict_from_cookiejar(cj)
    with open(filePath, "w") as f:
        d = requests.utils.dict_from_cookiejar(cj)
        f.write(json.dumps(d))


def loadCookies(filePath):
    with open(filePath, "r") as f:
        d = json.loads(f.read())
    return requests.utils.cookiejar_from_dict(d)


class ParseKwargs(argparse.Action):
    # https://sumit-ghosh.com/articles/parsing-dictionary-key-value-pairs-kwargs-argparse-python/
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, dict())
        for value in values:
            key, value = value.split("=")
            getattr(namespace, self.dest)[key] = value


parser = argparse.ArgumentParser(
    formatter_class=argparse.RawTextHelpFormatter,
    description="""
    ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ webdav-yike                                       ┃
    ┃    A webdav server for yikexiangce (baiduphoto)   ┃
    ┃                                                   ┃
    ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
""",
)

parser.add_argument("cjfile", type=str, default=None, nargs="*", help="data")
parser.add_argument(
    "-c", "--configcookies", help="create cookie_file for server", required=False
)
parser.add_argument("-o", "--output", help="specify output file", required=False)
parser.add_argument(
    "-p",
    "--port",
    help="server port",
    type=int,
    default=5000,
    required=False,
)
parser.add_argument(
    "-P",
    "--proxy",
    help="proxy to internet",
    type=str,
    required=False,
)
parser.add_argument(
    "-u",
    "--user",
    help="<user>:<password>",
    type=str,
    required=False,
)
parser.add_argument(
    "-O", "--option", nargs="*", help="-o key1=val1 key2=val2 ...", action=ParseKwargs
)


args = vars(parser.parse_args())
logging.debug(args)


if args["configcookies"] is not None:
    browser = args["configcookies"].lower()
    if hasattr(browser_cookie3, browser):
        func = getattr(browser_cookie3, browser)
        cj = func(domain_name=".baidu.com")
        outputFile = args["output"] if args["output"] is not None else "cj.json"
        dumpCookies(cj=cj, filePath=outputFile)
    else:
        logging.error(
            "cannot recognize [{}], details see:\n https://github.com/borisbabic/browser_cookie3#contribute".format(
                browser
            )
        )
    exit()


if args["user"] is None:
    user_mapping = {"*": True}
else:
    assert ":" in args["user"]
    user, password = args["user"].split(":")
    user_mapping = {"*": {user: {"password": password}}}


if len(args["cjfile"]) == 0:
    logging.error("need to input cjfile")
    exit()
else:
    cjFile = args["cjfile"][0]
    cj = loadCookies(cjFile)

    if args["proxy"] is not None:
        proxies = {"https": args["proxy"]}
    else:
        proxies = None

    api = API(cookies=cj, proxies=proxies)


sysConfig = dict(sysConfig_default)
if args["option"] is not None:
    for k in args["option"]:
        if k in sysConfig:
            TYPE = type(sysConfig[k])
            sysConfig[k] = TYPE(args["option"][k])


config = wsgidav_app.DEFAULT_CONFIG.copy()
config.update(
    {
        "host": "0.0.0.0",
        "port": args["port"],
        "provider_mapping": {"/": Provider(sysConfig, api)},
        "simple_dc": {
            "user_mapping": user_mapping,
        },
        "verbose": 5,
    }
)


app = WsgiDAVApp(config)

server_args = {
    "bind_addr": (config["host"], config["port"]),
    "wsgi_app": app,
}
server = wsgi.Server(**server_args)
server.start()
