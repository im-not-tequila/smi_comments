import asyncio
import bs4

from urllib3 import disable_warnings
from time import time
from threading import Thread
from queue import Queue

from functions.settings import Settings
from functions.parse_functions import sprint
from functions.parse_functions import Instruction, CommentsInstructions, NewsItem
from functions.parse_functions import MainLogInfo, ResourceLogInfo, Logger
from functions.parse_functions import CommentsParseFunctions


disable_warnings()


class MyQueue(Queue):
    def __init__(self):
        super(MyQueue, self).__init__()
        self.completed_count = 0

    def task_done(self):
        self.completed_count += 1
        super(MyQueue, self).task_done()

    def get_task_count(self):
        return self.completed_count


class CommentsParser:
    def __init__(self):
        self.func = CommentsParseFunctions()
        self.is_can_work: bool = self.func.check_connections()

        self.log = Logger()
        self.main_log_info: MainLogInfo = self.log.main_create()
        self.resource_log_info: ResourceLogInfo = ResourceLogInfo()

        self.msg = ''

        self.custom_functions = {
            'get_web_page_117002': self.func.get_web_page_117002,
            'get_comment_date_124444': self.func.get_comment_date_124444,
        }

    def run(self):
        try:
            instructions = CommentsInstructions().get()
            self.main_log_info.resources_count = len(instructions)

            for instruction in instructions:
                self.resource_log_info = self.log.resource_create(self.main_log_info.log_id, instruction.resource_id)
                news_items = self.func.get_resource_items(instruction.resource_id)

                self.resource_log_info.resource_id = instruction.resource_id
                self.resource_log_info.links_count = len(news_items)

                if instruction.page_load_type == 'selenium' or instruction.page_load_type == 'facebook_plugin':
                    self.get_soup_obj_selenium(news_items, instruction)

                futures = [self.parse_item(instruction, item) for item in news_items]
                loop = asyncio.get_event_loop()
                loop.run_until_complete(asyncio.wait(futures))

                self.log.resource_close(self.resource_log_info)

            self.log.main_close(self.main_log_info)

        except Exception as e:
            sprint('[GLOBAL ERROR] ' + str(e))
            self.main_log_info.unknown_exceptions_count += 1
            self.log.write(f'[GLOBAL ERROR] [resource_id: {self.resource_log_info.resource_id}] {str(e)}')
            self.log.main_close(self.main_log_info)
            self.log.resource_close(self.resource_log_info)
            exit(0)

    async def parse_item(self, instruction: Instruction, item: NewsItem):
        msg = f'\n***********************************************************************\n\n'
        msg += f'[RESOURCE ID: {instruction.resource_id}] {item.link}\n'

        if item.page_soup_obj is None:
            item.page_soup_obj = await self.get_soup_obj(item.link, instruction)

        comment_blocks = self.func.get_comment_blocks(item.page_soup_obj, instruction.blocks_xpath)
        comments_count = len(comment_blocks)

        self.main_log_info.comments_count += comments_count
        self.resource_log_info.comments_count += comments_count

        msg += f'[COMMENTS COUNT] {str(comments_count)}\n\n'

        for comment_block in comment_blocks:
            msg += f'---------------------------------------------------\n'
            comment_data = await self.parse_comment_block(comment_block, instruction)
            msg = await self.insert_comment(item.item_id, comment_data, msg)

        sprint(msg)

    async def get_soup_obj(self, link: str, instruction: Instruction) -> bs4.element:
        if instruction.template_link != '':
            link = self.func.get_item_link(instruction.template_link, link)

        if instruction.page_load_type == 'custom':
            data = {
                'link': link,
                'encoding': instruction.encoding
            }
            custom_function_name = 'get_web_page_' + str(instruction.resource_id)
            page = self.custom_functions[custom_function_name](data)

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

    def get_soup_obj_selenium(self, news_items: list[NewsItem], instruction: Instruction):
        max_threads = Settings.SELENIUM_MAX_THREADS
        tasks_count = len(news_items)

        if tasks_count < max_threads:
            max_threads = tasks_count

        threads: list[Thread] = []
        items_q = MyQueue()

        for item in news_items:
            items_q.put(item)

        sprint(f'\r[SELENIUM] Uploaded successfully: 0/{tasks_count}', end='')

        for i in range(max_threads):
            args: tuple = (
                instruction,
                items_q,
                tasks_count
            )
            thread = Thread(target=self.get_soup_obj_selenium_thread, args=args)
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    def get_soup_obj_selenium_thread(self, instruction: Instruction, items_q: MyQueue, tasks_count: int):
        while not items_q.empty():
            item = items_q.get()
            link = item.link

            if instruction.page_load_type == 'facebook_plugin':
                link = self.func.get_facebook_item_link(item.link)

            page = self.func.get_web_page_by_selenium(link)

            soup_page = self.func.get_item_soup(page)
            general_soup_object = soup_page

            if instruction.general_block_xpath != '':
                general_soup_object = self.func.get_general_comment_block(
                    soup_object=general_soup_object,
                    general_block_xpath=instruction.general_block_xpath
                )

            item.page_soup_obj = general_soup_object
            items_q.task_done()
            sprint(f'\r[SELENIUM] Uploaded successfully: {items_q.get_task_count()}/{tasks_count}', end='')

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
            date = self.custom_functions[custom_function_name](comment_block, instruction.date_format)
        else:
            date = self.func.get_comment_pubdate(comment_block, instruction.date_xpath, instruction.date_format)

        content = self.func.get_comment_data(comment_block, instruction.content_xpath)

        comment_data['author'] = author
        comment_data['date'] = date
        comment_data['content'] = content

        return comment_data

    async def insert_comment(self, item_id: int, comment_data: dict, msg: str) -> str:
        author = str(comment_data['author'])
        date = comment_data['date']
        content = str(comment_data['content'])

        if author == '' or content == '' or date['nd_date'] == 0:
            msg += f'\n!!! [BAD COMMENT]\n'
            self.main_log_info.bad_comments_count += 1
            self.resource_log_info.bad_comments_count += 1

            return msg

        title = self.func.escape_data(author)
        content = self.func.escape_data(content)
        is_comment_in_db = self.func.check_comment(item_id, title, content)

        msg += f"[AUTHOR] {author}\n[DATE] {date['human_date']}\n[CONTENT] {content}\n"

        query = """
                   INSERT IGNORE INTO comments_items (item_id, author, content, nd_date, s_date, not_date) 
                   VALUES (%s, %s, %s, %s, UNIX_TIMESTAMP(), %s)
                """

        params = (item_id, title, content, date['nd_date'], date['not_date'])

        if not is_comment_in_db:
            self.func.insert_comment(query, params)
            self.main_log_info.added_comments_count += 1
            self.resource_log_info.added_comments_count += 1
            msg += '[ADDED]\n'

        return msg


if __name__ == "__main__":
    comments_parser = CommentsParser()

    if not comments_parser.is_can_work:
        exit(0)

    time_start = time()
    comments_parser.run()
    time_finish = time()
    sprint()
    sprint(f'Total working time: {time_finish - time_start}')
