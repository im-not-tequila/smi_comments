import asyncio
import aiohttp
import time
import requests
import bs4
import selenium.common.exceptions
import io
from bs4 import BeautifulSoup
import dateparser
from datetime import datetime, timedelta
from pytz import utc
from time import mktime
from lxml import html
from pymysql.converters import escape_string
from .db import DataBase

from selenium import webdriver
from selenium.webdriver.firefox.options import Options


class CommentsParseFunctions:
    def __init__(self, main_db_info, tmp_db_info):
        self.TIMEOUT = 15
        self.HEADERS = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:45.0) Gecko/20100101 Firefox/45.0'
        }

        self.main_db = DataBase()
        self.tmp_db = DataBase()

        self.main_db.DB_INFO = main_db_info
        self.tmp_db.DB_INFO = tmp_db_info

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

    def get_resource_items(self, resource_id: int) -> tuple:
        date_from = datetime.now() - timedelta(3)
        date_from = date_from.strftime("%Y-%m-%d")

        # query = 'select id,link from items where res_id = %d and not_date >= "%s"' % (resource_id, date_from)
        query = f'select id, link from items where res_id = %s'
        params = (resource_id,)

        result = self.main_db.query_get(query, params)

        return result

    def get_item_link(self, template_link: str, item_link: str) -> str:
        item_id = item_link.split('/')[-1]
        link = template_link + item_id

        return link

    def get_facebook_item_link(self, item_link: str) -> str:
        print(item_link)
        #exit(0)
        page = requests.get(url=item_link, timeout=self.TIMEOUT)
        actual_url = page.url

        link = 'https://www.facebook.com/plugins/feedback.php?app_id=' \
               '182983179146255&container_width=672&height=100&href=' + actual_url + \
               '&locale=ru_RU&numposts=6&sdk=joey&version=v2.12'

        return link

    async def get_web_page(self, link: str, encoding: str) -> str:
        async with aiohttp.ClientSession(headers=self.HEADERS) as session:
            try:
                async with session.get(link, timeout=self.TIMEOUT) as resp:
                    file = await resp.read()
                    if resp.status == 200:
                        return file.decode(encoding=encoding, errors='replace')
            except aiohttp.client.ClientConnectorSSLError:
                async with session.get(link, timeout=self.TIMEOUT, ssl=False) as resp:
                    file = await resp.read()
                    if resp.status == 200:
                        return file.decode(encoding=encoding, errors='replace')
            except Exception as e:
                print(f'[ERROR] {str(e)}; link: {link}')

                return ''
        return ''

        # try:
        #     result = requests.get(url=link, timeout=self.TIMEOUT, headers=self.HEADERS)
        # except requests.exceptions.SSLError:
        #     result = requests.get(url=link, timeout=self.TIMEOUT, headers=self.HEADERS, verify=False)
        # except Exception as e:
        #     print('[ERROR] ' + str(e))
        #     return ''
        #
        # if encoding != '':
        #     result.encoding = encoding
        #
        # return result.text

    def get_web_page_117002(self, data: dict) -> str:
        item_id = data['link'].split('-')[-1]

        if item_id[-1] == '/':
            item_id = item_id[:-1]

        try:
            result = requests.get(url='http://vesti.kz/news/get/comments/?id=' + item_id,
                                  timeout=self.TIMEOUT,
                                  headers=self.HEADERS,)

            return result.text
        except Exception as e:
            print('[ERROR] ' + str(e))
            return ''

    def get_web_page_by_selenium(self, url: str) -> str:
        options = Options()
        options.add_argument('-headless')

        driver = webdriver.Firefox(executable_path='./selenium_driver/geckodriver', options=options)
        driver.set_page_load_timeout(self.TIMEOUT)

        try:
            driver.get(url)
            try:
                driver.execute_script("window.scrollBy({top: document.body.scrollHeight, behavior: 'smooth'});")
                time.sleep(self.TIMEOUT // 2)
            except selenium.common.exceptions.JavascriptException as e:
                print('[JS ERROR] ' + str(e))
        except selenium.common.exceptions.TimeoutException:
            try:
                driver.execute_script("window.scrollBy({top: document.body.scrollHeight, behavior: 'smooth'});")
                time.sleep(5)
            except selenium.common.exceptions.JavascriptException as e:
                print('[JS ERROR] ' + str(e))
            driver.execute_script("window.stop();")
        except Exception as e:
            print('[ERROR] ' + str(e))
            driver.quit()
            return ''

        page = driver.page_source
        driver.quit()
        # print(url)
        # print(page)
        # exit(0)

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

    def create_log(self, log_name: str) -> dict:
        log_info = {}
        start_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

        query = f'INSERT INTO comments_logs (start_time,file) VALUES (%s,%s)'
        params = (start_time, log_name)
        log_id = self.tmp_db.query_send(query, params)

        log_info['log_id'] = log_id
        log_info['start_time'] = start_time

        return log_info

    def close_log(self, log_info: dict):
        start_time = datetime.strptime(log_info['start_time'], '%Y-%m-%d %H:%M:%S')
        finish_time = datetime.today()
        finish_time_str = finish_time.strftime('%Y-%m-%d %H:%M:%S')
        duration = finish_time.timestamp() - start_time.timestamp()

        query = f'UPDATE comments_logs SET ' \
                f'finish_time = %s, ' \
                f'duration = %s, ' \
                f'total_resources_count = %s, ' \
                f'total_comments_count = %s, ' \
                f'added_comments_count = %s, ' \
                f'unknown_exceptions_count = %s ' \
                f'WHERE log_id = %s'

        params = (finish_time_str, duration, log_info['total_resources_count'], log_info['total_comments_count'],
                  log_info['added_comments_count'], log_info['unknown_exceptions_count'], log_info['log_id'])

        self.tmp_db.query_send(query, params)

    def escape_data(self, string: str) -> str:
        return escape_string(string)

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
            # print(bsoup)
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
        # print(pubdate)
        # exit(0)

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
