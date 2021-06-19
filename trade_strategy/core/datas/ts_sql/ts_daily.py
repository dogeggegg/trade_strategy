# coding:utf-8
import pymysql
import pandas as pd
import datetime
import io

from logger.logger import *
from core.datas.ts_sql.ts_config import TsConfig


class TsDaily(TsConfig):

    sql_columns_dict = {
        '日期': 'ts_trade_date',
        '证券代码': 'ts_code',
        '证券名称': 'ts_name',
        '地域': 'ts_area',
        '所属行业': 'ts_industry',
        '市场类型': 'ts_market',
        '开盘价': 'ts_open',
        '最高价': 'ts_high',
        '最低价': 'ts_low',
        '收盘价': 'ts_close',
        '前收盘价': 'ts_pre_close',
        '涨跌额': 'ts_change',
        '涨跌幅': 'ts_pct_chg',
        '成交量': 'ts_vol',
        '成交额': 'ts_amount',
        '换手率': 'ts_turnover_rate',
        '换手率(自由流通股)': 'ts_turnover_rate_f',
        '量比': 'ts_volume_ratio',
        '市盈率': 'ts_pe',
        '滚动市盈率': 'ts_pe_ttm',
        '市净率': 'ts_pb',
        '市销率': 'ts_ps',
        '滚动市销率': 'ts_ps_ttm',
        '股息率': 'ts_dv_ratio',
        '滚动股息率': 'ts_dv_ttm',
        '总股本': 'ts_total_share',
        '流通股本': 'ts_float_share',
        '自由流通股本': 'ts_free_share',
        '总市值': 'ts_total_mv',
        '流通市值': 'ts_circ_mv',
    }
    df_columns_dict = {
        'ts_trade_date': '日期',
        'ts_code': '证券代码',
        'ts_name': '证券名称',
        'ts_area': '地域',
        'ts_industry': '所属行业',
        'ts_market': '市场类型',
        'ts_open': '开盘价',
        'ts_high': '最高价',
        'ts_low': '最低价',
        'ts_close': '收盘价',
        'ts_pre_close': '前收盘价',
        'ts_change': '涨跌额',
        'ts_pct_chg': '涨跌幅',
        'ts_vol': '成交量',
        'ts_amount': '成交额',
        'ts_turnover_rate': '换手率',
        'ts_turnover_rate_f': '换手率(自由流通股)',
        'ts_volume_ratio': '量比',
        'ts_pe': '市盈率',
        'ts_pe_ttm': '滚动市盈率',
        'ts_pb': '市净率',
        'ts_ps': '市销率',
        'ts_ps_ttm': '滚动市销率',
        'ts_dv_ratio': '股息率',
        'ts_dv_ttm': '滚动股息率',
        'ts_total_share': '总股本',
        'ts_float_share': '流通股本',
        'ts_free_share': '自由流通股本',
        'ts_total_mv': '总市值',
        'ts_circ_mv': '流通市值'
    }

    def create_ts_daily_date_table(self):
        show_sql = (
            "SHOW TABLES LIKE 'ts_daily';"
        )
        result = self.execute_sql(show_sql)
        if not result:
            self.set_update_daily_time(None)
        #       ['证券代码', '日期', '开盘价', '最高价', '最低价', '收盘价', '前收盘价', '涨跌额', '涨跌幅',
        #        '成交量', '成交额', '换手率', '换手率(自由流通股)', '量比', '市盈率', '滚动市盈率',
        #        '市净率', '市销率', '滚动市销率', '股息率', '滚动股息率', '总股本', '流通股本',
        #        '自由流通股本', '总市值', '流通市值']
        create_ts_daily_date_table_sql = (
            "CREATE TABLE IF NOT EXISTS ts_daily("
            "ts_trade_date DATE,"
            "ts_code VARCHAR(20),"
            "ts_name VARCHAR(20),"
            "ts_area VARCHAR(20),"
            "ts_industry VARCHAR(20),"
            "ts_market VARCHAR(20),"
            "ts_open float,"
            "ts_high float,"
            "ts_low float,"
            "ts_close float,"
            "ts_pre_close float,"
            "ts_change float,"
            "ts_pct_chg float,"
            "ts_vol float,"
            "ts_amount float,"
            "ts_turnover_rate float,"
            "ts_turnover_rate_f float,"
            "ts_volume_ratio float,"
            "ts_pe float,"
            "ts_pe_ttm float,"
            "ts_pb float,"
            "ts_ps float,"
            "ts_ps_ttm float,"
            "ts_dv_ratio float,"
            "ts_dv_ttm float,"
            "ts_total_share float,"
            "ts_float_share float,"
            "ts_free_share float,"
            "ts_total_mv float,"
            "ts_circ_mv float,"
            "UNIQUE KEY ts_code_trade_date(ts_code, ts_trade_date), "
            "INDEX ts_code(ts_code), "
            "INDEX ts_trade_date(ts_trade_date)"
            ");")
        self.execute_sql(create_ts_daily_date_table_sql)

    def add_ts_daily(self, df):
        """
        添加一行个股数据到数据库
        :param df:
        :return:
        """
        df.rename(columns=self.sql_columns_dict, inplace=True)
        df.to_sql('ts_daily', self.engine, if_exists='append', index=False, method='multi')

    def delete_ge_trade_date_ts_daily(self, trade_date):
        """
        删除 大于等于这个日期的数据， 即删除这个时间点之后的数据
        :param trade_date:
        :return:
        """
        delete_sql = (
            f"DELETE FROM ts_daily WHERE ts_trade_date>='{trade_date}';"
        )
        self.execute_sql(delete_sql)

    def get_ts_daily(self, start_date, end_date, ts_code=None,
                     filters=None, order=None, limit=None):
        """
        获取指定日期区间的 指定个股数据
        :param start_date: '2021-06-10'
        :param end_date:   '2021-06-10'
        :param ts_code:  证券代码, None为全部
        :param filters: 过滤字符
        :param order:   排序
        :param limit: 0,10   前十条0~10条
        :return:
        """
        if start_date == end_date:
            sql = (
                f"SELECT * FROM ts_daily "
                f"WHERE ts_trade_date='{end_date}';"
            )
        else:
            sql = (
                f"SELECT * FROM ts_daily "
                f"WHERE date(ts_trade_date) between '{start_date}' AND '{end_date}';"
            )
        if ts_code:
            sql = sql.strip(';') + f" AND ts_code='{ts_code}';"
        if filters:
            field, include = filters
            if include:     # 包含
                sql = sql.strip(';') + f" AND LOCATE('{field}', ts_name);"
            else:           # 去除，不包含
                sql = sql.strip(';') + f" AND LOCATE('{field}', ts_name)=0;"
        if order:
            field, sort = order
            sql = sql.strip(';') + f" ORDER BY {self.sql_columns_dict[field]} {sort};"
        if limit:
            sql = sql.strip(';') + f" LIMIT {limit};"
        self.conn.connect()
        df = pd.read_sql(sql, self.conn)
        self.conn.close()
        df.rename(columns=self.df_columns_dict, inplace=True)
        return df

    def get_ts_daily_ts_code(self, start_date, end_date):
        """
        获取一段时间内的 ts_code
        :param start_date:
        :param end_date:
        :return:
        """
        sql = (
            f"SELECT ts_code FROM ts_daily "
            f"WHERE '{start_date}'<=ts_trade_date AND ts_trade_date<='{end_date}' "
            f"GROUP BY ts_code;"
        )
        self.conn.connect()
        df = pd.read_sql(sql, self.conn)
        self.conn.close()
        ts_code_list = list(df['ts_code'])
        return ts_code_list




