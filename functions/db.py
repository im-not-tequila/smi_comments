import pymysql


class DataBase:
    def __init__(self):
        self.DB_INFO = {}
        self.CONSOLE = True

    def create_connection(self):
        try:
            con = pymysql.connect(host=self.DB_INFO['host'],
                                  port=self.DB_INFO['port'],
                                  user=self.DB_INFO['user'],
                                  passwd=self.DB_INFO['password'],
                                  db=self.DB_INFO['db'],
                                  charset='utf8',
                                  cursorclass=pymysql.cursors.DictCursor)
            return con
        except pymysql.err.OperationalError as e:
            if self.CONSOLE:
                print('[ERROR] ' + str(e))
            exit(0)

    def query_get(self, query: str, params: tuple = ()) -> tuple:
        con = self.create_connection()

        with con.cursor() as cur:
            cur.execute(query, params)
            result = cur.fetchall()

        return result

    def query_send(self, query: str, params: tuple = ()):
        con = self.create_connection()
        _id = 0

        try:
            with con.cursor() as cur:
                cur.execute(query, params)
                con.commit()
                _id = int(cur.lastrowid)
        finally:
            con.close()

        return _id

    def query_send_stack(self, query: str, params: list[tuple]):
        con = self.create_connection()

        try:
            with con.cursor() as cur:
                cur.executemany(query, params)
                con.commit()
        finally:
            con.close()
