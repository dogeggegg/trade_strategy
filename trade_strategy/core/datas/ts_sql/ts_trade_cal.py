# coding:utf-8

import pymysql
import pandas as pd
import datetime


from core.datas.ts_sql.ts_config import TsConfig


class TsTradeCal(TsConfig):

    def create_ts_trade_cal_date_table(self):
        show_sql = (
            "SHOW TABLES LIKE 'ts_trade_cal';"
        )
        result = self.execute_sql(show_sql)
        if not result:
            self.set_update_trade_cal_time(None)
        # 表 ts_trade_cal
        create_ts_trade_cal_date_table_sql = (
            "CREATE TABLE IF NOT EXISTS ts_trade_cal("
            "exchange VARCHAR(128),"
            "cal_date DATE,"
            "is_open enum('休市', '交易'),"
            "UNIQUE KEY cal_date_unique(cal_date)"
            ");"
        )
        self.execute_sql(create_ts_trade_cal_date_table_sql)

    def get_trade_cal(self, start_date, end_date=None):
        if end_date is None:
            end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        sql = (
            f"SELECT * FROM ts_trade_cal "
            f"WHERE is_open='交易' "
            f"AND date(cal_date) between '{start_date}' AND '{end_date}' "
            f";"
        )
        self.conn.connect()
        df = pd.read_sql(sql, self.conn)
        self.conn.close()
        df.rename(columns={
            'exchange': '交易所',  # 交易所 SSE上交所 SZSE深交所
            'cal_date': '日历日期',
            'is_open': '是否交易'  # 是否交易 0休市 1交易
        }, inplace=True)
        return df

    def add_trade_cal(self, df):
        df.rename(columns={
            '交易所': 'exchange',  # 交易所 SSE上交所 SZSE深交所
            '日历日期': 'cal_date',
            '是否交易': 'is_open'  # 是否交易 0休市 1交易
        }, inplace=True)
        df.to_sql('ts_trade_cal', self.engine, if_exists='append', index=False)



