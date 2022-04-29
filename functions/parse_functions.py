import os
import sys
import aiohttp
import time
import requests
import bs4
import selenium.common.exceptions
import re
import dateparser

from .settings import Settings
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from pytz import utc
from time import mktime
from lxml import html
from pymysql.converters import escape_string
from .db import DataBase
from contextlib import contextmanager

from selenium import webdriver
from selenium.webdriver.firefox.options import Options


@contextmanager
def suppress_stdout():
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout


def sprint(*a, **b):
    if Settings.CONSOLE:
        print(*a, **b)


class FunctionsDataBase:
    def __init__(self):
        self.main_db = DataBase()
        self.main_db.DB_INFO = Settings.MAIN_DB_INFO

        self.tmp_db = DataBase()
        self.tmp_db.DB_INFO = Settings.TMP_DB_INFO


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


class ParseCommentsInstructions(FunctionsDataBase):
    def __init__(self):
        super().__init__()

        self.query = """
                        SELECT * FROM comments_instructions
                        WHERE page_load_type = 'facebook_plugin'
                     """
        #NOT page_load_type = 'facebook_plugin'
    def get(self) -> list:
        instructions_elements = self.tmp_db.query_get(self.query)
        instructions: list = []

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
                instruction.blocks_xpath = 'div:::class:::UFIImageBlockContent'
                instruction.content_xpath = '1::del::div:::class:::_3-8m'
                instruction.date_xpath = '2::del::data-utime=":::"'
                instruction.author_xpath = '3::del:://span[1]/text()'

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


class Logger(FunctionsDataBase):
    def __init__(self):
        super().__init__()

        self.name = 'comments_parser'
        self.time_format = '%Y-%m-%d %H:%M:%S'

    def main_create(self) -> dict:
        log_info = {}
        start_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

        query = """
                   INSERT INTO comments_main_logs (start_time,file) 
                   VALUES (%s,%s)
                """
        params = (start_time, self.name)
        log_id = self.tmp_db.query_send(query, params)

        log_info['log_id'] = log_id
        log_info['start_time'] = start_time

        self.write('[INFO] the parser is running')

        return log_info

    def resource_create(self, main_log_id: int, resource_id: int) -> dict:
        log_info = {}
        start_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

        query = """
                   INSERT INTO comments_resource_logs (main_log_id, resource_id, start_time) 
                   VALUES (%s, %s, %s)
                """
        params = (main_log_id, resource_id, start_time)
        log_id = self.tmp_db.query_send(query, params)

        log_info['log_id'] = log_id
        log_info['start_time'] = start_time

        return log_info

    def write(self, string: str):
        with open(self.name + '.log', 'a') as f:
            current_time = datetime.today().strftime(self.time_format)
            f.write(f'{current_time} | {string}\n')

    def main_close(self, log_info: dict):
        start_time = datetime.strptime(log_info['start_time'], '%Y-%m-%d %H:%M:%S')
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
            log_info['resources_count'],
            log_info['comments_count'],
            log_info['bad_comments_count'],
            log_info['added_comments_count'],
            log_info['unknown_exceptions_count'],
            log_info['log_id']
        )

        self.tmp_db.query_send(query, params)
        self.write('[INFO] the parser is completed')

    def resource_close(self, log_info: dict):
        start_time = datetime.strptime(log_info['start_time'], '%Y-%m-%d %H:%M:%S')
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
                  log_info['links_count'],
                  log_info['comments_count'],
                  log_info['bad_comments_count'],
                  log_info['added_comments_count'],
                  log_info['unknown_exceptions_count'],
                  log_info['log_id'])

        self.tmp_db.query_send(query, params)


class CommentsParseFunctions(FunctionsDataBase):
    def __init__(self):
        super().__init__()

    def get_resources(self) -> tuple:
        query = 'select * from comment_settings'
        # query = 'select * from comment_settings where id = 10'
        result = self.tmp_db.query_get(query)

        return result

    def check_connections(self) -> bool:
        query = "SHOW PROCESSLIST"
        base_connections = self.tmp_db.query_get(query)
        connections = 0

        for connection in base_connections:
            from_ip = connection['Host'].split(':')
            if connection['User'] == 'comments_parser' and from_ip[0] == '94.247.130.37':
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

    def get_item_link(self, template_link: str, item_link: str) -> str:
        item_id = item_link.split('/')[-1]
        link = template_link + item_id

        return link

    def get_facebook_item_link(self, item_link: str) -> str:
        page = requests.get(url=item_link, timeout=Settings.TIMEOUT)
        actual_url = page.url

        link = 'https://www.facebook.com/plugins/feedback.php?app_id=' \
               '182983179146255&container_width=672&height=100&href=' + actual_url + \
               '&locale=ru_RU&numposts=6&sdk=joey&version=v2.12'

        return link

    async def get_web_page(self, link: str, encoding: str) -> str:
        async with aiohttp.ClientSession(headers=Settings.HEADERS) as session:
            try:
                async with session.get(link, timeout=Settings.TIMEOUT) as resp:
                    file = await resp.read()
                    if resp.status == 200:
                        return file.decode(encoding=encoding, errors='replace')
            except aiohttp.client.ClientConnectorCertificateError:
                async with session.get(link, timeout=Settings.TIMEOUT, ssl=False) as resp:
                    file = await resp.read()
                    if resp.status == 200:
                        return file.decode(encoding=encoding, errors='replace')
                return ''
        return ''

    def get_web_page_117002(self, data: dict) -> str:
        item_id = data['link'].split('-')[-1]

        if item_id[-1] == '/':
            item_id = item_id[:-1]

        try:
            result = requests.get(url='http://vesti.kz/news/get/comments/?id=' + item_id,
                                  timeout=Settings.TIMEOUT,
                                  headers=Settings.HEADERS,)

            return result.text
        except Exception as e:
            sprint('[ERROR] ' + str(e))
            return ''

    # async def get_web_page_by_selenium(self, url: str) -> str:
    #     browser_params = {'moz:firefoxOptions': {
    #         'args': ['']
    #     }}
    #
    #     service = services.Geckodriver(binary="./selenium_driver/geckodriver", log_file=os.devnull)
    #     browser = browsers.Firefox(**browser_params)
    #
    #     with suppress_stdout():
    #         async with get_session(service, browser) as session:
    #             await session.get(url)
    #             await session.execute_script("window.scrollBy({top: document.body.scrollHeight, behavior: 'smooth'});")
    #             time.sleep(self.TIMEOUT // 2)
    #             page = await session.get_page_source()
    #
    #     return page

    def get_web_page_by_selenium(self, url: str) -> str:
        sprint(f'[SELENIUM LOAD] {url}\n')
        options = Options()
        options.add_argument('-headless')

        driver = webdriver.Firefox(executable_path='./selenium_driver/geckodriver', options=options)
        driver.set_page_load_timeout(Settings.TIMEOUT_SELENIUM)

        try:
            driver.get(url)
            try:
                driver.execute_script("window.scrollBy({top: document.body.scrollHeight, behavior: 'smooth'});")
                time.sleep(Settings.TIMEOUT_SELENIUM // 2)
            except selenium.common.exceptions.JavascriptException as e:
                sprint('[JS ERROR] ' + str(e))
        except selenium.common.exceptions.TimeoutException:
            try:
                driver.execute_script("window.scrollBy({top: document.body.scrollHeight, behavior: 'smooth'});")
                time.sleep(5)
            except selenium.common.exceptions.JavascriptException as e:
                sprint('[JS ERROR] ' + str(e))
            driver.execute_script("window.stop();")
        except Exception as e:
            sprint('[ERROR] ' + str(e))
            driver.quit()
            return ''

        page = driver.page_source
        driver.quit()

        return page

    def get_item_soup(self, item_page: str) -> BeautifulSoup:
        return BeautifulSoup(item_page, "html.parser")

    def get_general_comment_block(self, soup_object: bs4.element, general_block_xpath: str) -> bs4.element:
        general_block_xpath = general_block_xpath.split(':::')
        tag = general_block_xpath[0]
        attribute = general_block_xpath[1]
        value = general_block_xpath[2]

        if value in ['true', 'false']:
            element = soup_object.find(tag)
        else:
            element = soup_object.find(tag, {attribute: value})

        return element

    def get_comment_blocks(self, soup_object: bs4.element, blocks_xpath: str) -> bs4.element:
        blocks_xpath_split = blocks_xpath.split(':::')
        tag = blocks_xpath_split[0]
        attribute = blocks_xpath_split[1]
        values = blocks_xpath_split[2].split('::$or::')
        elements = []

        for value in values:
            if value in ['true', 'false']:
                elements += soup_object.find_all(tag)
            else:
                elements += soup_object.find_all(tag, {attribute: value})

        #чистка блоков от вложенных комментов
        for value in values:
            for el in elements:
                nested_comments = el.find_all(tag, {attribute: value})

                if nested_comments:
                    for comment in nested_comments:
                        comment.extract()

        return elements

    def get_comment_fb_author(self, bsoup: bs4.element) -> str:
        element = bsoup.find('a', {'class': 'UFICommentActorName'})

        if element is not None:
            return element.text
        else:
            element = bsoup.find('span')
            if element is not None:
                return element.text
            else:
                return ''

    def check_comment(self, item_id: int, title: str, content: str) -> bool:
        sql_query = f'select id from comments_items where item_id = %s and author = %s and content = %s'
        _params = (item_id, title, content)

        result = self.tmp_db.query_get(sql_query, _params)

        if len(result) == 1:
            return True

        return False

    def escape_data(self, string: str) -> str:
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

    def insert_comment(self, query: str, params: tuple = ()):
        self.tmp_db.query_send(query, params)

    def get_comment_data(self, bsoup: bs4.element, content_xpath: str) -> str:
        parse_method = content_xpath.split('::del::')[0]
        xpath_info = content_xpath.split('::del::')[1]

        if parse_method == '1':
            content_xpath = xpath_info.split(':::')
            tag = content_xpath[0]
            attribute = content_xpath[1]
            values = content_xpath[2].split('::$or::')
            element = None

            for value in values:
                element = bsoup.find(tag, {attribute: value})
                if element:
                    break

            if element is None:
                return ''

            content = element.text.strip()

            return content

        if parse_method == '2':
            xpath_info = xpath_info.split(':::')
            top_tag = xpath_info[0]
            bottom_tag = xpath_info[1]
            bsoup = str(bsoup)
            top_tag_position = bsoup.find(top_tag)

            if top_tag_position != -1:
                element = bsoup[top_tag_position:]
                bottom_tag_position = element.find(bottom_tag)

                if bottom_tag_position != -1:
                    content = element[:bottom_tag_position]

                    return content

        if parse_method == '3':
            tree = html.fromstring(str(bsoup))
            content = tree.xpath(xpath_info[0])[0]

            return content

        return ''

    def get_comment_pubdate(self, bsoup: bs4.element, date_xpath: str, date_format: str) -> dict:
        date_info = {
            'not_date': '',
            'nd_date': 0,
            'human_date': ''
        }

        if date_xpath == '':
            pubdate = datetime.now()
            not_date = f'{pubdate.year}-{pubdate.month}-{pubdate.day}'
            human_date = f'{pubdate.year}-{pubdate.month}-{pubdate.day} ' \
                         f'{pubdate.hour}:{pubdate.minute}:{pubdate.second}'

            nd_date = int(mktime(utc.localize(pubdate).utctimetuple()))

            date_info['not_date'] = not_date
            date_info['nd_date'] = nd_date
            date_info['human_date'] = human_date

            return date_info

        xpath_info = date_xpath.split('::del::')
        parse_method = xpath_info[0]
        xpath_info = xpath_info[1:]

        if parse_method == '1':
            date_xpath = xpath_info[0].split(':::')
            element = bsoup.find(date_xpath[0], {date_xpath[1]: date_xpath[2]})

        elif parse_method == '2':
            element = None
            xpath_info = xpath_info[0].split(':::')
            top_tag = xpath_info[0]
            bottom_tag = xpath_info[1]
            bsoup = str(bsoup)
            top_tag_position = bsoup.find(top_tag)

            if top_tag_position != -1:
                top_tag_position += len(top_tag)
                element = bsoup[top_tag_position:]
                bottom_tag_position = element.find(bottom_tag)

                if bottom_tag_position != -1:
                    element = element[:bottom_tag_position]
                    element = self.get_item_soup(element)
                else:
                    element = None

        elif parse_method == '3':
            date_xpath = xpath_info[0].split(':::')

            if date_xpath[1] == 'false':
                element = bsoup.find(date_xpath[0])
            else:
                element = bsoup.find(date_xpath[0], {date_xpath[1]: date_xpath[2]})

            element = element[date_xpath[3]]
            element = self.get_item_soup(element)

        else:
            return {}

        if element is None:
            return date_info

        pubdate = element.text.strip()
        pubdate = self.date_replacer(pubdate)

        if date_format != '':
            pubdate = dateparser.parse(pubdate, settings={'DATE_ORDER': date_format})
        else:
            pubdate = dateparser.parse(pubdate)

        if pubdate is None:
            return date_info

        pubdate_str = f'{pubdate.year}-{pubdate.month}-{pubdate.day} {pubdate.hour}:{pubdate.minute}:{pubdate.second}'

        test_pubdate = dateparser.parse(pubdate_str)
        current_date = datetime.now()

        if test_pubdate > current_date:
            return date_info

        current_time = datetime.now()

        if pubdate is None:
            pubdate = datetime.now()

        hour = pubdate.hour
        minute = pubdate.minute
        second = pubdate.second

        if hour == 0 and minute == 0:
            hour = current_time.hour
            minute = current_time.minute
            second = current_time.second

        not_date = f'{pubdate.year}-{pubdate.month}-{pubdate.day}'
        human_date = f'{pubdate.year}-{pubdate.month}-{pubdate.day} {hour}:{minute}:{second}'

        pubdate = dateparser.parse(human_date)
        nd_date = int(mktime(utc.localize(pubdate).utctimetuple()))
        date_info['not_date'] = not_date
        date_info['nd_date'] = nd_date
        date_info['human_date'] = human_date

        return date_info

    def get_comment_date_124444(self, bsoup: bs4.element.Tag, date_format):
        date_info = {}
        try:
            date_in_span = bsoup.find_all('span')[2]
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

    def date_replacer(self, date_str: str) -> str:
        replace_word_1 = {'понедельник': 'monday',
                          'вторник': 'tuesday',
                          'среда': 'wednesday',
                          'четверг': 'thursday',
                          'пятница': 'friday',
                          'суббота': 'saturday',
                          'воскресенье': 'sunday',

                          'января': 'january',
                          'февраля': 'february',
                          'марта': 'march',
                          'апреля': 'april',
                          'мая': 'may',
                          'июня': 'june',
                          'июля': 'july',
                          'августа': 'august',
                          'сентября': 'september',
                          'октября': 'october',
                          'ноября': 'november',
                          'декабря': 'december',

                          'қаңтар': 'jan',
                          'ақпан': 'feb',
                          'наурыз': 'mar',
                          'сәуір': 'apr',
                          'мамыр': 'may',
                          'маусым': 'jun',
                          'шілде': 'jul',
                          'тамыз': 'aug',
                          'қыркүйек': 'sept',
                          'қазан': 'oct',
                          'қараша': 'nov',
                          'желтоқсан': 'dec',

                          'ж.': 'year',
                          'г.': 'year'
                          }

        date_str = date_str.lower()

        for key, value in replace_word_1.items():
            date_str = date_str.replace(key, value)

        return date_str
