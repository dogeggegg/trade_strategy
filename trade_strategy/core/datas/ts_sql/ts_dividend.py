# coding:utf-8

import numpy as np
import pandas as pd
import datetime
import traceback

from logger.logger import *
from core.datas.ts_sql.ts_config import TsConfig


class TsDividend(TsConfig):

    def create_ts_dividend_date_table(self):
        show_sql = (
            "SHOW TABLES LIKE 'ts_dividend';"
        )
        result = self.execute_sql(show_sql)
        if not result:
            self.set_update_dividend_time(None)
        # 表 ts_dividend
        create_ts_dividend_date_table_sql = (
            "CREATE TABLE IF NOT EXISTS ts_dividend("
            "ts_code VARCHAR(20),"
            "dividPreNoticeDate DATE,"
            "dividAgmPumDate DATE,"
            "dividPlanAnnounceDate DATE,"
            "dividPlanDate DATE,"
            "dividRegistDate DATE,"
            "dividOperateDate DATE,"
            "dividPayDate DATE,"
            "dividStockMarketDate DATE,"
            "dividCashPsBeforeTax float,"
            "dividCashPsAfterTax VARCHAR(200),"
            "dividStocksPs float,"
            "dividCashStock VARCHAR(200),"
            "dividReserveToStockPs float,"
            "UNIQUE KEY ts_code_dividOperateDate(ts_code, dividOperateDate)"
            ");"
        )
        self.execute_sql(create_ts_dividend_date_table_sql)

    def get_ts_dividend(self, start_date, end_date=None):
        # todo
        return
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

    def delete_ge_trade_date_ts_dividend(self, trade_date):
        delete_sql = (
            f"DELETE FROM ts_dividend WHERE dividOperateDate>='{trade_date}';"
        )
        self.execute_sql(delete_sql)

    def add_ts_dividend(self, df):
        """
        增加一年所有个股的分红数据
        :param df:
        :return:
        """
        df.rename(columns={
            '证券代码': 'ts_code',
            '预批露公告日': 'dividPreNoticeDate',
            '股东大会公告日期': 'dividAgmPumDate',
            '预案公告日': 'dividPlanAnnounceDate',
            '分红实施公告日': 'dividPlanDate',
            '股权登记告日': 'dividRegistDate',
            '除权除息日期': 'dividOperateDate',
            '派息日': 'dividPayDate',
            '红股上市交易日': 'dividStockMarketDate',
            '每股股利税前': 'dividCashPsBeforeTax',
            '每股股利税后': 'dividCashPsAfterTax',
            '每股红股': 'dividStocksPs',
            '分红送转': 'dividCashStock',
            '每股转增资本': 'dividReserveToStockPs',
        }, inplace=True)
        df.replace('', np.nan, inplace=True)
        try:
            df.to_sql('ts_dividend', self.engine, if_exists='append', index=False)
        except Exception:
            printe(f'\n{df}')
            raise



