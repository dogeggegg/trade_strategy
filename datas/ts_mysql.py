# coding:utf-8
import pymysql


class TsMySQL:

    host = 'localhost'
    port = 3306
    user = 'root'
    passwd = '123456'
    db = 'tushare'
    connect_timeout = 1

    def execute_sql(self, sql):
        conn = pymysql.connect(host=self.host,
                               port=self.port,
                               user=self.user,
                               passwd=self.passwd,
                               db=self.db,
                               connect_timeout=self.connect_timeout)
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        conn.commit()
        cursor.close()
        conn.close()
        return results

    def execute_many_sql(self, sql_list):
        conn = pymysql.connect(host=self.host,
                               port=self.port,
                               user=self.user,
                               passwd=self.passwd,
                               db=self.db,
                               connect_timeout=self.connect_timeout)
        cursor = conn.cursor()
        results = list()
        for sql in sql_list:
            cursor.execute(sql)
            results.append(cursor.fetchall())
        conn.commit()
        cursor.close()
        conn.close()
        return results

    def 


ts_mysql = TsMySQL()
