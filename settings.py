import configparser
import json
from functions.parse_functions import CommentsParseFunctions


class Settings:
    CONSOLE = True
    STATUS = 'comments_parser'
    TIMEOUT = 15

    config = configparser.ConfigParser()
    config.read('connections.ini')

    MAIN_DB_INFO = json.loads(config['CONNECTIONS_INFO']['MAIN_DB'])
    TMP_DB_INFO = json.loads(config['CONNECTIONS_INFO']['TMP_DB'])

    func = CommentsParseFunctions(MAIN_DB_INFO, TMP_DB_INFO)
    CUSTOM_FUNCTIONS = {
        'get_web_page_117002': func.get_web_page_117002,
        'get_comment_date_124444': func.get_comment_date_124444,
    }
