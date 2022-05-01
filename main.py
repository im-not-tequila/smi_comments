import warnings
import asyncio
import bs4

from pytz_deprecation_shim import PytzUsageWarning
from urllib3 import disable_warnings
from threading import Thread
from queue import Queue
from time import time

from functions.settings import Settings
from functions.parse_functions import sprint
from functions.parse_functions import Comment, Instruction, CommentsInstructions, NewsItem
from functions.parse_functions import MainLogInfo, ResourceLogInfo, Logger
from functions.parse_functions import CommentsParseFunctions

warnings.filterwarnings(action="ignore", category=PytzUsageWarning)
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

        self.custom_functions = {
            'get_web_page_117002': self.func.get_web_page_117002,
            'get_comment_date_124444': self.func.get_comment_date_124444,
        }

    # запуск парсера
    def run(self):
        try:
            instructions = CommentsInstructions().get()
            self.main_log_info.resources_count = len(instructions)

            for instruction in instructions:
                self.resource_log_info = self.log.resource_create(self.main_log_info.log_id, instruction.resource_id)
                news_items = self.func.get_resource_items(instruction.resource_id)

                self.resource_log_info.resource_id = instruction.resource_id
                self.resource_log_info.links_count = len(news_items)

                # асинхронная загрузка страниц
                if instruction.page_load_type == 'selenium' or instruction.page_load_type == 'facebook_plugin':
                    self.start_threads_selenium(news_items, instruction)
                else:
                    futures = [self.set_soup_obj(item, instruction) for item in news_items]
                    loop = asyncio.get_event_loop()
                    loop.run_until_complete(asyncio.wait(futures))

                for item in news_items:
                    self.parse_item(item, instruction)

                good_comments_count = len(instruction.comments)

                if good_comments_count > 0:
                    self.func.insert_comments(instruction.comments)

                self.main_log_info.added_comments_count += good_comments_count
                self.resource_log_info.added_comments_count += good_comments_count

                self.log.resource_close(self.resource_log_info)

            self.log.main_close(self.main_log_info)

        except Exception as e:
            sprint('[GLOBAL ERROR] ' + str(e))
            self.main_log_info.unknown_exceptions_count += 1
            self.log.write(f'[GLOBAL ERROR] [resource_id: {self.resource_log_info.resource_id}] {str(e)}')
            self.log.main_close(self.main_log_info)
            self.log.resource_close(self.resource_log_info)
            exit(0)

    # устанавливает объектам items их параметр page_soup_obj (страница или часть страницы со всеми комментариями)
    async def set_soup_obj(self, item: NewsItem, instruction: Instruction):
        link = item.link

        if instruction.template_link != '':
            link = self.func.get_item_link(instruction.template_link, link)

        if instruction.page_load_type == 'custom':
            data = {
                'link': link,
                'encoding': instruction.encoding
            }
            custom_function_name = 'get_web_page_' + str(instruction.resource_id)
            soup_page = self.custom_functions[custom_function_name](data)

        else:
            soup_page = await self.func.get_web_page(link, instruction.encoding)

        general_soup_object = soup_page

        if instruction.general_block_xpath != '':
            general_soup_object = self.func.get_general_comment_block(
                soup_object=general_soup_object,
                general_block_xpath=instruction.general_block_xpath
            )

        item.page_soup_obj = general_soup_object

    # запускает загрузку страниц через selenium в потоках
    def start_threads_selenium(self, news_items: list[NewsItem], instruction: Instruction):
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
            thread = Thread(target=self.set_soup_obj_selenium_thread, args=args)
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    # тело потока. так же устанавливает объектам items их параметр page_soup_obj
    def set_soup_obj_selenium_thread(self, instruction: Instruction, items_q: MyQueue, tasks_count: int):
        while not items_q.empty():
            item = items_q.get()

            if instruction.page_load_type == 'facebook_plugin':
                item.link = self.func.get_facebook_item_link(item.link)

            soup_page = self.func.get_web_page_selenium(item.link)

            general_soup_object = soup_page

            if instruction.general_block_xpath != '':
                general_soup_object = self.func.get_general_comment_block(
                    soup_object=general_soup_object,
                    general_block_xpath=instruction.general_block_xpath
                )

            item.page_soup_obj = general_soup_object
            items_q.task_done()
            sprint(f'\r[SELENIUM] Uploaded successfully: {items_q.get_task_count()}/{tasks_count}', end='')

    # выполняет инструкцию для одной ссылки. формирует список объектов комментариев, которые будут добавлены
    def parse_item(self, item: NewsItem, instruction: Instruction):
        sprint(f'\n***********************************************************************\n')
        sprint(f'[RESOURCE ID: {instruction.resource_id}] {item.link}')

        comment_blocks = self.func.get_comment_blocks(item.page_soup_obj, instruction.blocks_xpath)
        comments_count = len(comment_blocks)

        self.main_log_info.comments_count += comments_count
        self.resource_log_info.comments_count += comments_count

        sprint(f'[COMMENTS COUNT] {str(comments_count)}\n')

        for comment_block in comment_blocks:
            sprint(f'---------------------------------------------------')
            comment: Comment = self.parse_comment_block(comment_block, instruction)
            comment.item_id = item.item_id
            sprint(f'[AUTHOR] {comment.author}')
            sprint(f'[DATE] {comment.human_date}')
            sprint(f'[CONTENT] {comment.content}')

            if self.check_comment(comment):
                instruction.comments.append(comment)

    # разбирает блок комментария. возвращает объект одного комментария
    def parse_comment_block(self, comment_block: bs4.element, instruction: Instruction) -> Comment:
        comment: Comment = Comment()

        author = self.func.get_comment_data(comment_block, instruction.author_xpath)

        if instruction.is_custom_get_date:
            custom_function_name = 'get_comment_date_' + str(instruction.resource_id)
            date = self.custom_functions[custom_function_name](comment_block, instruction.date_format)
        else:
            date_str = self.func.get_comment_data(comment_block, instruction.date_xpath)
            date = self.func.str_to_date(date_str, instruction.date_format)

        content = self.func.get_comment_data(comment_block, instruction.content_xpath)

        comment.author = self.func.escape_data(author)
        comment.content = self.func.escape_data(content)
        comment.nd_date = date['nd_date']
        comment.not_date = date['not_date']
        comment.human_date = date['human_date']

        return comment

    # валидация комментария
    def check_comment(self, comment: Comment) -> bool:
        if comment.author == '' or comment.content == '' or comment.nd_date == 0:
            self.main_log_info.bad_comments_count += 1
            self.resource_log_info.bad_comments_count += 1
            sprint()
            sprint(f'!!! [BAD COMMENT]')

            return False

        is_comment_in_db = self.func.check_comment(comment)

        if is_comment_in_db:
            return False

        sprint()
        sprint(f'[WILL BE ADDED]')

        return True


if __name__ == "__main__":
    comments_parser = CommentsParser()

    if not comments_parser.is_can_work:
        exit(0)

    time_start = time()
    comments_parser.run()
    time_finish = time()
    sprint()
    sprint(f'Total working time: {time_finish - time_start}')
