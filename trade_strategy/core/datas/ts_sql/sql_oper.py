# coding:utf-8
import pymysql
from sqlalchemy import create_engine

from logger.logger import *


class SQLOper(object):

    host = 'localhost'
    port = 3306
    user = 'root'
    passwd = '123456'
    db = 'tushare'
    connect_timeout = 1

    engine = create_engine(f'mysql+pymysql://{user}:{passwd}@{host}:{port}/{db}')

    def __init__(self):
        # 创建数据库
        self.create_datebase()
        self.conn = pymysql.connect(host=self.host,
                                    port=self.port,
                                    user=self.user,
                                    passwd=self.passwd,
                                    db=self.db,
                                    connect_timeout=self.connect_timeout)

    def windup(self):
        try:
            self.conn.close()
        except Exception as e:
            printe(str(e))

    def create_datebase(self):
        create_database_sql = (
            f"CREATE DATABASE IF NOT EXISTS {self.db};"
        )
        conn = pymysql.connect(host=self.host,
                               port=self.port,
                               user=self.user,
                               passwd=self.passwd,
                               connect_timeout=self.connect_timeout)
        cursor = conn.cursor()
        cursor.execute(create_database_sql)
        cursor.fetchall()
        conn.commit()
        cursor.close()
        conn.close()

    def execute_sql(self, sql):
        self.conn.connect()
        cursor = self.conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        self.conn.commit()
        cursor.close()
        self.conn.close()
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

