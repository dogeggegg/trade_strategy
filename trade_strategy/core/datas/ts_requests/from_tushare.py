# coding:utf-8

import pandas as pd
import tushare as ts

from logger.logger import *


class FromTushare(object):

    ts.set_token('54f042d586a61d1911fe7a8fd4f0e8324995eb37f157af0300e24cd2')
    pro = ts.pro_api(timeout=5)

    def get_trade_cal(self, start_date, end_date=None):
        """
        获取各大交易所交易日历数据
        :param start_date: 开始日期 （格式：YYYY-MM-DD 下同），'20180101'
        :param end_date: 结束日期， None表示今天
        :return: list ['2020-01-01', '2020-01-02']
        """
        kwargs = {'start_date': start_date.replace('-', '')}
        if end_date:
            kwargs['end_date'] = end_date.replace('-', '')
        df = self.pro.query('trade_cal', **kwargs)
        df['cal_date'] = ['-'.join([d[:4], d[4:6], d[6:8]]) for d in df['cal_date']]
        df.rename(columns={
            'exchange': '交易所',      # 交易所 SSE上交所 SZSE深交所
            'cal_date': '日历日期',
            'is_open': '是否交易'       # 是否交易 0休市 1交易
        }, inplace=True)
        mapping = {'SSE': '上交所', 'SZSE': '深交所'}
        df['交易所'] = df['交易所'].map(mapping)
        mapping = {0: '休市', 1: '交易'}
        df['是否交易'] = df['是否交易'].map(mapping)
        return df

    def get_stock_basic(self):
        """
        获取证券名称 df
        :return:
        """
        df = self.pro.query('stock_basic',
                            exchange='',  # 交易所 SSE 上交所 SZSE 深交所 HKEX 港交所
                            list_status='L',  # 上市状态 L上市 D退市 P暂停上市
                            fields='ts_code,symbol,name,area,industry,market,list_date')
        df['ts_code'] = ['.'.join([c[-2:].lower(), c[:6]]) for c in list(df['ts_code'])]
        df.rename(columns={
            'ts_code': '证券代码',
            'symbol': '股票代码',
            'name': '证券名称',
            'area': '地域',
            'industry': '所属行业',
            'market': '市场类型',       # （主板/创业板/科创板/CDR）
            'list_date': '上市日期',
        }, inplace=True)
        return df

    def get_daily(self, trade_date):
        """
        获取 日线行情
        :param trade_date:
        :return:
        """
        kwargs = dict()
        trade_date = trade_date.replace('-', '')
        kwargs['start_date'] = trade_date
        kwargs['end_date'] = trade_date
        df = self.pro.query('daily', **kwargs)
        df['ts_code'] = ['.'.join([c[-2:].lower(), c[:6]]) for c in list(df['ts_code'])]
        df.rename(columns={'trade_date': '日期',
                           'ts_code': '证券代码',
                           'open': '开盘价',
                           'high': '最高价',
                           'low': '最低价',
                           'close': '收盘价',
                           'pre_close': '前收盘价',
                           'change': '涨跌额',
                           'pct_chg': '涨跌幅',
                           'vol': '成交量',
                           'amount': '成交额',
                           }, inplace=True)
        return df

    def get_daily_basic(self, trade_date):
        """
        返回某一天的全股 基本面指标
        :param trade_date:  "2020-08-04"
        :return:
        """
        df = self.pro.daily_basic(trade_date=trade_date.replace('-', ''))
        df['ts_code'] = ['.'.join([c[-2:].lower(), c[:6]]) for c in list(df['ts_code'])]
        df.rename(columns={'trade_date': '日期',
                           'ts_code': '证券代码',
                           'close': '收盘价',
                           'turnover_rate': '换手率',
                           'turnover_rate_f': '换手率(自由流通股)',
                           'volume_ratio': '量比',
                           'pe': '市盈率',
                           'pe_ttm': '滚动市盈率',
                           'pb': '市净率',
                           'ps': '市销率',
                           'ps_ttm': '滚动市销率',
                           'dv_ratio': '股息率',
                           'dv_ttm': '滚动股息率',
                           'total_share': '总股本',
                           'float_share': '流通股本',
                           'free_share': '自由流通股本',
                           'total_mv': '总市值',
                           'circ_mv': '流通市值'
                           }, inplace=True)
        return df


if __name__ == '__main__':
    from_tushare = FromTushare()
    # print(from_tushare.get_ts_date_daily('20210525', '20210525'))
    # print(from_tushare.get_trade_cal('20200101'))
    # print(from_tushare.get_all_ts_code(contains_st=False))
    # from_tushare.get_trade_cal('1980-01-01')
