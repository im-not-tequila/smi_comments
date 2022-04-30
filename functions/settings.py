import configparser
import json


class Settings:
    CONSOLE = True
    STATUS = 'comments_parser'
    TIMEOUT = 40
    TIMEOUT_SELENIUM = 25
    SELENIUM_MAX_THREADS = 12
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:45.0) Gecko/20100101 Firefox/45.0'
    }

    config = configparser.ConfigParser()
    config.read('connections.ini')

    MAIN_DB_INFO = json.loads(config['CONNECTIONS_INFO']['MAIN_DB'])
    TMP_DB_INFO = json.loads(config['CONNECTIONS_INFO']['TMP_DB'])

    # func = CommentsParseFunctions()
    # CUSTOM_FUNCTIONS = {
    #     'get_web_page_117002': func.get_web_page_117002,
    #     'get_comment_date_124444': func.get_comment_date_124444,
    # }
