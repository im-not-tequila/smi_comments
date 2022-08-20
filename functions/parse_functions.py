import dateparser
import requests
import aiohttp
import bs4
import re

from pymysql.converters import escape_string
from datetime import datetime, timedelta
from .settings import Settings
from bs4 import BeautifulSoup
from .db import DataBase
from time import mktime, sleep
from pytz import utc

import selenium.common.exceptions
from selenium.webdriver.firefox.options import Options
from selenium import webdriver


def sprint(*a, **b):
    if Settings.CONSOLE:
        print(*a, **b)


class FunctionsDataBase:
    def __init__(self):
        self.main_db = DataBase()
        self.main_db.DB_INFO = Settings.MAIN_DB_INFO

        self.tmp_db = DataBase()
        self.tmp_db.DB_INFO = Settings.TMP_DB_INFO


class Comment:
    def __init__(self):
        self.item_id = 0
        self.author = None
        self.content = None
        self.nd_date = 0
        self.s_date = 0
        self.not_date = None
        self.human_date = None


class Instruction:
    def __init__(self):
        self.resource_id = None
        self.template_link = None
        self.page_load_type = None
        self.encoding = None
        self.general_block_xpath = None
        self.blocks_xpath = None
        self.author_xpath = None
        self.content_xpath = None
        self.date_xpath = None
        self.date_format = None
        self.is_custom_get_date = None
        self.comments: list[Comment] = []


class CommentsInstructions(FunctionsDataBase):
    def __init__(self):
        super().__init__()

        self.query = Settings.QUERY_FOR_GET_INSTRUCTIONS

    def get(self) -> list:
        instructions_elements = self.tmp_db.query_get(self.query)
        instructions: list[Instruction] = []

        for el in instructions_elements:
            instruction = Instruction()

            instruction.resource_id = el['resource_id']
            instruction.template_link = el['url']
            instruction.page_load_type = el['page_load_type']
            instruction.general_block_xpath = el['general_block_xpath']
            instruction.blocks_xpath = el['blocks_xpath']
            instruction.content_xpath = el['content_xpath']
            instruction.date_xpath = el['date_xpath']
            instruction.author_xpath = el['author_xpath']
            instruction.encoding = el['encoding']
            instruction.date_format = el['date_format']
            instruction.is_custom_get_date = el['get_date_custom']

            if instruction.page_load_type == 'facebook_plugin':
                instruction.general_block_xpath = 'div:::class:::_4k-6'
                instruction.blocks_xpath = 'div:::direction:::left'
                instruction.content_xpath = '1::del::div:::class:::_3-8m'
                instruction.date_xpath = '2::del::data-utime=":::"'
                instruction.author_xpath = '1::del::(a:::class:::UFICommentActorName)::' \
                                           '$or::(span:::class:::UFICommentActorName)'

            if instruction.encoding == '':
                instruction.encoding = 'utf8'

            instructions.append(instruction)

        return instructions


class NewsItem:
    def __init__(self):
        self.item_id = None
        self.link = None
        self.page_soup_obj = None

    def __str__(self):
        text = ''
        text += f'[ID] {self.item_id}\n'
        text += f'[LINK] {self.link}\n'

        if self.page_soup_obj is not None:
            text += f'[PAGE] {str(self.page_soup_obj)[:150]}...\n'
        else:
            text += f'[PAGE] {self.page_soup_obj}\n'

        return text


class MainLogInfo:
    def __init__(self):
        self.log_id = None
        self.start_time = None
        self.finish_time = None
        self.duration = None
        self.file = None
        self.resources_count = 0
        self.comments_count = 0
        self.bad_comments_count = 0
        self.added_comments_count = 0
        self.unknown_exceptions_count = 0


class ResourceLogInfo:
    def __init__(self):
        self.log_id = None
        self.main_log_id = None
        self.resource_id = None
        self.start_time = None
        self.finish_time = None
        self.duration = 0
        self.links_count = 0
        self.comments_count = 0
        self.bad_comments_count = 0
        self.added_comments_count = 0
        self.unknown_exceptions_count = 0


class Logger(FunctionsDataBase):
    def __init__(self):
        super().__init__()

        self.name = 'comments_parser'
        self.time_format = '%Y-%m-%d %H:%M:%S'

    def main_create(self) -> MainLogInfo:
        main_log_info = MainLogInfo()
        start_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

        query = """
                   INSERT INTO comments_main_logs (start_time,file) 
                   VALUES (%s,%s)
                """
        params = (start_time, self.name)
        log_id = self.tmp_db.query_send(query, params)

        main_log_info.log_id = log_id
        main_log_info.start_time = start_time

        self.write('[INFO] the parser is running')

        return main_log_info

    def resource_create(self, main_log_id: int, resource_id: int) -> ResourceLogInfo:
        resource_log_info = ResourceLogInfo()
        start_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

        query = """
                   INSERT INTO comments_resource_logs (main_log_id, resource_id, start_time) 
                   VALUES (%s, %s, %s)
                """
        params = (main_log_id, resource_id, start_time)
        log_id = self.tmp_db.query_send(query, params)

        resource_log_info.log_id = log_id
        resource_log_info.start_time = start_time

        return resource_log_info

    def write(self, string: str):
        with open(self.name + '.log', 'a') as f:
            current_time = datetime.today().strftime(self.time_format)
            f.write(f'{current_time} | {string}\n')

    def main_close(self, main_log_info: MainLogInfo):
        start_time = datetime.strptime(main_log_info.start_time, '%Y-%m-%d %H:%M:%S')
        finish_time = datetime.today()
        finish_time_str = finish_time.strftime('%Y-%m-%d %H:%M:%S')
        duration = finish_time.timestamp() - start_time.timestamp()

        query = """
                   UPDATE comments_main_logs SET
                   finish_time = %s,
                   duration = %s,
                   resources_count = %s,
                   comments_count = %s,
                   bad_comments_count = %s,
                   added_comments_count = %s,
                   unknown_exceptions_count = %s
                   WHERE log_id = %s
                """

        params = (
            finish_time_str,
            duration,
            main_log_info.resources_count,
            main_log_info.comments_count,
            main_log_info.bad_comments_count,
            main_log_info.added_comments_count,
            main_log_info.unknown_exceptions_count,
            main_log_info.log_id
        )

        self.tmp_db.query_send(query, params)
        self.write('[INFO] the parser is completed')

    def resource_close(self, resource_log_info: ResourceLogInfo):
        start_time = datetime.strptime(resource_log_info.start_time, '%Y-%m-%d %H:%M:%S')
        finish_time = datetime.today()
        finish_time_str = finish_time.strftime('%Y-%m-%d %H:%M:%S')
        duration = finish_time.timestamp() - start_time.timestamp()

        query = """
                   UPDATE comments_resource_logs SET 
                   finish_time = %s, 
                   duration = %s, 
                   links_count = %s, 
                   comments_count = %s, 
                   bad_comments_count = %s, 
                   added_comments_count = %s, 
                   unknown_exceptions_count = %s 
                   WHERE log_id = %s
                """

        params = (finish_time_str,
                  duration,
                  resource_log_info.links_count,
                  resource_log_info.comments_count,
                  resource_log_info.bad_comments_count,
                  resource_log_info.added_comments_count,
                  resource_log_info.unknown_exceptions_count,
                  resource_log_info.log_id)

        self.tmp_db.query_send(query, params)


class CommentsParseFunctions(FunctionsDataBase):
    def __init__(self):
        super().__init__()

    def check_connections(self) -> bool:
        query = "SHOW PROCESSLIST"
        base_connections = self.tmp_db.query_get(query)
        connections = 0

        for connection in base_connections:
            from_ip = connection['Host'].split(':')
            if connection['User'] == 'comments_parser' and from_ip[0] == '':
                connections += 1

        if connections > 1:
            return False
        else:
            return True

    def get_resource_items(self, resource_id: int) -> list[NewsItem]:
        date_from = datetime.now() - timedelta(3)
        date_from = date_from.strftime("%Y-%m-%d")

        # query = 'select id,link from items where res_id = %d and not_date >= "%s"' % (resource_id, date_from)
        query = f'select id, link from items where res_id = %s'
        params = (resource_id,)

        result = self.main_db.query_get(query, params)
        news_items: list[NewsItem] = []

        for el in result:
            item = NewsItem()
            item.item_id = el['id']
            item.link = el['link']
            news_items.append(item)

        return news_items

    @staticmethod
    def get_item_link(template_link: str, item_link: str) -> str:
        item_id = item_link.split('/')[-1]
        link = template_link + item_id

        return link

    @staticmethod
    def get_facebook_item_link(item_link: str) -> str:
        page = requests.get(url=item_link, timeout=Settings.TIMEOUT)
        actual_url = page.url

        link = 'https://www.facebook.com/plugins/feedback.php?app_id=' \
               '182983179146255&container_width=672&height=100&href=' + actual_url + \
               '&locale=ru_RU&numposts=6&sdk=joey&version=v2.12'

        return link

    @staticmethod
    async def get_web_page(link: str, encoding: str) -> BeautifulSoup:
        page_str: str = ''

        async with aiohttp.ClientSession(headers=Settings.HEADERS) as session:
            try:
                async with session.get(link, timeout=Settings.TIMEOUT) as resp:
                    file = await resp.read()
                    if resp.status == 200:
                        page_str = file.decode(encoding=encoding, errors='replace')

            except aiohttp.client.ClientConnectorCertificateError:
                async with session.get(link, timeout=Settings.TIMEOUT, ssl=False) as resp:
                    file = await resp.read()
                    if resp.status == 200:
                        page_str = file.decode(encoding=encoding, errors='replace')

        return BeautifulSoup(page_str, "html.parser")

    @staticmethod
    def get_web_page_selenium(link: str) -> BeautifulSoup:
        options = Options()
        options.add_argument('-headless')

        driver = webdriver.Firefox(executable_path=Settings.GECKODRIVER_PATH, options=options)
        driver.set_page_load_timeout(Settings.TIMEOUT_SELENIUM)

        try:
            driver.get(link)
            try:
                driver.execute_script("window.scrollBy({top: document.body.scrollHeight, behavior: 'smooth'});")
                sleep(5)
            except selenium.common.exceptions.JavascriptException as e:
                sprint('[JS ERROR] ' + str(e))
        except selenium.common.exceptions.TimeoutException:
            try:
                driver.execute_script("window.scrollBy({top: document.body.scrollHeight, behavior: 'smooth'});")
                sleep(5)
            except selenium.common.exceptions.JavascriptException as e:
                sprint('[JS ERROR] ' + str(e))
            driver.execute_script("window.stop();")
        except Exception as e:
            sprint('[ERROR] ' + str(e))
        finally:
            page_str = driver.page_source
            driver.quit()

            if page_str == '<html><head></head><body></body></html>':
                page_str = ''

            return BeautifulSoup(page_str, 'html.parser')

    @staticmethod
    def get_general_comment_block(soup_object: bs4.element, general_block_xpath: str) -> bs4.element:
        general_block_xpath = general_block_xpath.split(':::')
        tag = general_block_xpath[0]
        attribute = general_block_xpath[1]
        value = general_block_xpath[2]

        if value in ['true', 'false']:
            element = soup_object.find(tag)
        else:
            element = soup_object.find(tag, {attribute: value})

        return element

    @staticmethod
    def get_comment_blocks(soup_object: bs4.element, blocks_xpath: str) -> bs4.element:
        conditions = blocks_xpath.split('::$or::')
        elements = []

        for condition in conditions:
            condition = condition.replace('(', '').replace(')', '')
            condition_split = condition.split(':::')
            tag = condition_split[0]
            attribute = condition_split[1]
            value = condition_split[2]

            if value in ['true', 'false']:
                elements += soup_object.find_all(tag)
            else:
                elements += soup_object.find_all(tag, {attribute: value})

            # чистка блоков от вложенных комментариев
            for el in elements:
                nested_comments = el.find_all(tag, {attribute: value})

                if nested_comments:
                    for comment in nested_comments:
                        comment.extract()

        return elements

    def check_comment(self, comment: Comment) -> bool:
        sql_query = """
                       SELECT id FROM comments_items 
                       WHERE item_id = %s AND author = %s AND content = %s
                    """
        _params = (comment.item_id, comment.author, comment.content)

        result = self.tmp_db.query_get(sql_query, _params)

        if len(result) == 1:
            return True

        return False

    @staticmethod
    def escape_data(string: str) -> str:
        emoj = re.compile("["
                          u"\U0001F600-\U0001F64F"  # emoticons        
                          u"\U0001F300-\U0001F5FF"  # symbols & pictographs        
                          u"\U0001F680-\U0001F6FF"  # transport & map symbols        
                          u"\U0001F1E0-\U0001F1FF"  # flags (iOS)        
                          u"\U00002500-\U00002BEF"  # chinese char        
                          u"\U00002702-\U000027B0"
                          u"\U00002702-\U000027B0"
                          u"\U000024C2-\U0001F251"
                          u"\U0001f926-\U0001f937"
                          u"\U00010000-\U0010ffff"
                          u"\u2640-\u2642"
                          u"\u2600-\u2B55"
                          u"\u200d"
                          u"\u23cf"
                          u"\u23e9"
                          u"\u231a"
                          u"\ufe0f"  # dingbats        
                          u"\u3030"
                          "]+", re.UNICODE)

        s = re.sub(emoj, '', string)

        return escape_string(s)

    # добавляет пачку комментариев в базу
    def insert_comments(self, comments: list[Comment]):
        query = """
                   INSERT IGNORE INTO comments_items (item_id, author, content, nd_date, s_date, not_date) 
                   VALUES (%s, %s, %s, %s, UNIX_TIMESTAMP(), %s)
                """
        params: list[tuple] = []

        for comment in comments:
            param = (comment.item_id, comment.author, comment.content, comment.nd_date, comment.not_date)
            params.append(param)

        self.tmp_db.query_send_stack(query, params)

    @staticmethod
    def get_comment_data(soup_object: bs4.element, xpath: str) -> str:
        parse_method = xpath.split('::del::')[0]
        xpath_info = xpath.split('::del::')[1]

        if parse_method == '1':
            conditions = xpath_info.split('::$or::')

            for condition in conditions:
                condition = condition.replace('(', '').replace(')', '')
                condition_split = condition.split(':::')
                tag = condition_split[0]
                attribute = condition_split[1]
                value = condition_split[2]

                if value in ['$true', '$false']:
                    element = soup_object.find(tag)
                else:
                    element = soup_object.find(tag, {attribute: value})

                if element:
                    data = element.text.strip()
                    return data

        if parse_method == '2':
            condition_split = xpath_info.split(':::')

            begin = condition_split[0]
            end = condition_split[1]

            soup_object_str = str(soup_object)
            begin_position = soup_object_str.find(begin)

            if begin_position != -1:
                element = soup_object_str[begin_position + len(begin):]
                end_position = element.find(end)

                if end_position != -1:
                    el_str = element[:end_position]
                    el = BeautifulSoup(el_str)
                    data = el.text
                    return data

        elif parse_method == '3':
            condition_split = xpath_info.split(':::')
            tag = condition_split[0]
            attribute = condition_split[1]
            value = condition_split[2]
            return_attribute = condition_split[3]

            if attribute == '$false':
                element = soup_object.find(tag)
            else:
                element = soup_object.find(tag, {attribute: value})

            if element:
                date_str = str(element[return_attribute]).strip()
                return date_str

        return ''

    def str_to_date(self, date_str: str, date_format: str) -> dict:
        date_info = {
            'not_date': '',
            'nd_date': 0,
            'human_date': ''
        }

        date_str = self.date_replacer(date_str)
        current_date = datetime.now()

        if date_format != '':
            date = dateparser.parse(date_str, settings={'DATE_ORDER': date_format})
        else:
            date = dateparser.parse(date_str)

        if date is None:
            return date_info

        if date.timestamp() > current_date.timestamp():
            return date_info

        hour = date.hour
        minute = date.minute
        second = date.second

        if hour == 0 and minute == 0:
            hour = current_date.hour
            minute = current_date.minute
            second = current_date.second

        not_date = f'{date.year}-{date.month}-{date.day}'
        human_date = f'{date.year}-{date.month}-{date.day} {hour}:{minute}:{second}'

        nd_date = int(date.timestamp())

        date_info['not_date'] = not_date
        date_info['nd_date'] = nd_date
        date_info['human_date'] = human_date

        return date_info

    @staticmethod
    def date_replacer(date_str: str) -> str:
        replace_words = Settings.REPLACE_WORDS_FOR_DATE

        date_str = date_str.lower()

        for key, value in replace_words.items():
            date_str = date_str.replace(key, value)

        return date_str

    # CUSTOM FUNCTIONS

    @staticmethod
    def get_web_page_117002(data: dict) -> BeautifulSoup:
        item_id = data['link'].split('-')[-1]

        if item_id[-1] == '/':
            item_id = item_id[:-1]

        try:
            result = requests.get(url='http://vesti.kz/news/get/comments/?id=' + item_id,
                                  timeout=Settings.TIMEOUT,
                                  headers=Settings.HEADERS, )

            return BeautifulSoup(result.text, "html.parser")
        except Exception as e:
            sprint('[ERROR] ' + str(e))
            return BeautifulSoup('', "html.parser")

    @staticmethod
    def get_comment_date_124444(soup_object: bs4.element.Tag, date_format):
        date_info = {}
        try:
            date_in_span = soup_object.find_all('span')[2]
            a_in_span = date_in_span.find('a')
            span_in_span = date_in_span.find('span')
            a_in_span.decompose()
            span_in_span.decompose()
            pubdate_str = date_in_span.text.strip()[:-1].strip()
        except IndexError:
            date_info['not_date'] = ''
            date_info['nd_date'] = 0
            date_info['human_date'] = ''

            return date_info

        year = datetime.now().year
        month = pubdate_str.split('-')[0].split('/')[1].strip()
        day = pubdate_str.split('-')[0].split('/')[0].strip()
        hours = pubdate_str.split('-')[1].split(':')[0].strip()
        minutes = pubdate_str.split('-')[1].split(':')[1].strip()

        pubdate = datetime(int(year), int(month), int(day), int(hours), int(minutes))

        not_date = f'{year}-{month}-{day}'
        nd_date = int(mktime(utc.localize(pubdate).utctimetuple()))
        human_date = f'{year}-{month}-{day} {hours}:{minutes}:00'

        date_info['not_date'] = not_date
        date_info['nd_date'] = nd_date
        date_info['human_date'] = human_date

        return date_info
