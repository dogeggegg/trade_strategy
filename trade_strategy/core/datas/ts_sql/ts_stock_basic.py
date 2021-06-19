# coding:utf-8
import pymysql
import pandas as pd


from core.datas.ts_sql.ts_config import TsConfig


class TsStockBasic(TsConfig):

    def create_ts_stock_basic_date_table(self):
        # 表 ts_stock_basic
        create_ts_stock_basic_date_table_sql = (
            "CREATE TABLE IF NOT EXISTS ts_stock_basic("
            "ts_code VARCHAR(20),"
            "ts_symbol VARCHAR(20),"
            "ts_name VARCHAR(20),"
            "ts_area VARCHAR(20),"
            "ts_industry VARCHAR(20),"
            "ts_market VARCHAR(20),"
            "ts_list_date DATE,"
            "UNIQUE KEY ts_code_unique(ts_code)"
            ");"
        )
        self.execute_sql(create_ts_stock_basic_date_table_sql)

    def get_ts_stock_basic(self):
        sql = (
            f"SELECT * FROM ts_stock_basic;"
        )
        self.conn.connect()
        df = pd.read_sql(sql, self.conn)
        self.conn.close()
        df.rename(columns={
            'ts_code': '证券代码',
            'symbol': '股票代码',
            'name': '证券名称',
            'area': '地域',
            'industry': '所属行业',
            'market': '市场类型',
            'list_date': '上市日期',
        }, inplace=True)
        return df

    def add_stock_basic(self, df):
        df.rename(columns={
            '证券代码': 'ts_code',
            '股票代码': 'ts_symbol',
            '证券名称': 'ts_name',
            '地域': 'ts_area',
            '所属行业': 'ts_industry',
            '市场类型': 'ts_market',
            '上市日期': 'ts_list_date',
        }, inplace=True)
        df.to_sql('ts_stock_basic', self.engine, if_exists='replace', index=False)

    def get_all_industry(self):
        sql = (
            "SELECT ts_industry FROM ts_stock_basic GROUP BY ts_industry;"
        )
        self.conn.connect()
        df = pd.read_sql(sql, self.conn)
        self.conn.close()
        df.rename(columns={
            'ts_industry': '所属行业',
        }, inplace=True)
        df.dropna(subset=['所属行业'], inplace=True)
        industry_list = list(df['所属行业'])
        return industry_list

    def get_all_ts_code(self):
        sql = (
            "SELECT ts_code FROM ts_stock_basic GROUP BY ts_code;"
        )
        self.conn.connect()
        df = pd.read_sql(sql, self.conn)
        self.conn.close()
        df.rename(columns={
            'ts_code': '证券代码',
        }, inplace=True)
        ts_code_list = list(df['证券代码'])
        return ts_code_list
