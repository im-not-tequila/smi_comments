import asyncio
import urllib3
import bs4
from datetime import datetime
from settings import Settings
from functions.parse_functions import CommentsParseFunctions
from functions.db import DataBase


urllib3.disable_warnings()


def sprint(*a, **b):
    if Settings.CONSOLE:
        print(*a, **b)


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


class ParseCommentsInstructions:
    def __init__(self):
        self.tmp_db = DataBase()
        self.tmp_db.DB_INFO = Settings.TMP_DB_INFO
        self.query = "select * from comments_instructions where page_load_type = '' "
        #self.query = "select * from comments_instructions where id = 22"

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


class Logger:
    def __init__(self):
        self.tmp_db = DataBase()
        self.tmp_db.DB_INFO = Settings.TMP_DB_INFO

        self.name = 'comments_parser'
        self.time_format = '%Y-%m-%d %H:%M:%S'

    def create(self):
        log_info = {}
        start_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

        query = f'INSERT INTO comments_logs (start_time,file) VALUES (%s,%s)'
        params = (start_time, self.name)
        log_id = self.tmp_db.query_send(query, params)

        log_info['log_id'] = log_id
        log_info['start_time'] = start_time

        self.write('[INFO] the parser is running')

        return log_info

    def write(self, string: str):
        with open(self.name + '.log', 'a') as f:
            current_time = datetime.today().strftime(self.time_format)
            f.write(f'{current_time} | {string}')

    def close(self, log_info: dict):
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


class CommentsParser:
    def __init__(self):
        self.func = CommentsParseFunctions(main_db_info=Settings.MAIN_DB_INFO,
                                           tmp_db_info=Settings.TMP_DB_INFO)
        self.func.TIMEOUT = Settings.TIMEOUT
        self.is_can_work = self.func.check_connections()
        self.log_info = self.func.create_log(Settings.STATUS)

        self.total_resources_count = 0
        self.total_comments_count = 0
        self.added_comments_count = 0
        self.unknown_exceptions_count = 0

        self.msg = ''

    def run(self):
        try:
            instructions = ParseCommentsInstructions().get()
            self.total_resources_count = len(instructions)

            for instruction in instructions:
                news_items = self.func.get_resource_items(instruction.resource_id)

                futures = [self.parse_item(instruction, item) for item in news_items]
                loop = asyncio.get_event_loop()
                loop.run_until_complete(asyncio.wait(futures))

            self.func.close_log(self.get_log_info())

        except Exception as e:
            sprint('[GLOBAL ERROR] ' + str(e))
            self.unknown_exceptions_count += 1
            self.func.close_log(self.get_log_info())
            exit(0)

    async def parse_item(self, instruction, item):
        link = str(item['link'])
        item_id = item['id']

        msg = f'***********************************************************************\n\n{link}\n'

        soup_obj = await self.get_soup_obj(link, instruction)
        comment_blocks = self.func.get_comment_blocks(soup_obj, instruction.blocks_xpath)
        self.total_comments_count += len(comment_blocks)
        msg += f'[COMMENTS COUNT] {str(len(comment_blocks))}\n\n'

        for comment_block in comment_blocks:
            msg += f'---------------------------------------------------\n'
            comment_data = await self.parse_comment_block(comment_block, instruction)
            msg = await self.insert_comment(item_id, comment_data, msg)

        sprint(msg)

    async def get_soup_obj(self, link: str, instruction: Instruction) -> bs4.element:
        if instruction.template_link != '':
            link = self.func.get_item_link(instruction.template_link, link)

        if instruction.page_load_type == 'facebook_plugin':
            link = self.func.get_facebook_item_link(link)
            page = self.func.get_web_page_by_selenium(link)

        elif instruction.page_load_type == 'selenium':
            page = self.func.get_web_page_by_selenium(link)

        elif instruction.page_load_type == 'custom':
            data = {
                'link': link,
                'encoding': instruction.encoding
            }
            custom_function_name = 'get_web_page_' + str(instruction.resource_id)
            page = Settings.CUSTOM_FUNCTIONS[custom_function_name](data)

        else:
            page = await self.func.get_web_page(link, instruction.encoding)

        soup_page = self.func.get_item_soup(page)
        general_soup_object = soup_page

        if instruction.general_block_xpath != '':
            general_soup_object = self.func.get_general_comment_block(
                soup_object=general_soup_object,
                general_block_xpath=instruction.general_block_xpath
            )

        return general_soup_object

    async def parse_comment_block(self, comment_block: bs4.element, instruction: Instruction) -> dict:
        comment_data = {
            'author': '',
            'date': '',
            'content': ''
        }

        if instruction.page_load_type == 'facebook_plugin':
            author = self.func.get_comment_fb_author(comment_block)
        else:
            author = self.func.get_comment_data(comment_block, instruction.author_xpath)

        if instruction.is_custom_get_date:
            custom_function_name = 'get_comment_date_' + str(instruction.resource_id)
            date = Settings.CUSTOM_FUNCTIONS[custom_function_name](comment_block, instruction.date_format)
        else:
            date = self.func.get_comment_pubdate(comment_block, instruction.date_xpath, instruction.date_format)

        content = self.func.get_comment_data(comment_block, instruction.content_xpath)

        comment_data['author'] = author
        comment_data['date'] = date
        comment_data['content'] = content

        return comment_data

    async def insert_comment(self, item_id, comment_data, msg) -> str:
        author = str(comment_data['author'])
        date = comment_data['date']
        content = str(comment_data['content'])
        msg += f"[AUTHOR] {author}\n[DATE] {date['human_date']}\n[CONTENT] {content}\n"

        if author == '' or content == '' or date['nd_date'] == 0:
            return msg

        title = self.func.escape_data(author)
        content = self.func.escape_data(content)
        is_comment_in_db = self.func.check_comment(item_id, title, content)

        query = f'INSERT IGNORE INTO comments_items (item_id, author, content, nd_date, s_date, not_date) ' \
                'VALUES (%s, %s, %s, %s, UNIX_TIMESTAMP(), %s)'

        params = (item_id, title, content, date['nd_date'], date['not_date'])

        if not is_comment_in_db:
            self.func.insert_comment(query, params)
            self.added_comments_count += 1
            msg += '[ADDED]\n'

        return msg

    def get_log_info(self):
        log_info = {
            'log_id': self.log_info['log_id'],
            'start_time': self.log_info['start_time'],
            'total_resources_count': self.total_resources_count,
            'total_comments_count': self.total_comments_count,
            'added_comments_count': self.added_comments_count,
            'unknown_exceptions_count': self.unknown_exceptions_count
        }

        return log_info


if __name__ == "__main__":
    comments_parser = CommentsParser()

    if not comments_parser.is_can_work:
        exit(0)

    comments_parser.run()
