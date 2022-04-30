import configparser
import json
import platform


class Settings:
    CONSOLE = True
    STATUS = 'comments_parser'
    TIMEOUT = 40
    TIMEOUT_SELENIUM = 25
    SELENIUM_MAX_THREADS = 12
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:45.0) Gecko/20100101 Firefox/45.0'
    }

    if platform.system() == 'Windows':
        GECKODRIVER_PATH = './selenium_driver/geckodriver.exe'
    else:
        GECKODRIVER_PATH = './selenium_driver/geckodriver'

    config = configparser.ConfigParser()
    config.read('connections.ini')

    MAIN_DB_INFO = json.loads(config['CONNECTIONS_INFO']['MAIN_DB'])
    TMP_DB_INFO = json.loads(config['CONNECTIONS_INFO']['TMP_DB'])
