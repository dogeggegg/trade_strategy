# coding:utf-8

import baostock as bs
import pandas as pd

from logger.logger import *


class FromBaostock(object):

    login = False

    def init(self):
        if not self.login:
            bs.login()
            self.login = True

    def windup(self):
        bs.logout()
        self.login = False

    def query_history_k_data_plus(self, ts_code, start_date, end_date):
        """
        返回指定股票日线
        :param ts_code:     "sz.000001"
        :param start_date:  "2020-08-04"
        :param end_date:    "2020-08-04"
        :return:            pandas
            日期      股票代码  开盘价  最高价     最低价  收盘价  昨日收盘价     涨跌幅     成交量          成交额      换手率
           date       code     open     high      low    close preclose     pctChg    volume           amount      turn
0    2020-01-02  sz.002120  33.4900  33.5000  32.1500  32.9300  33.3000  -1.111100  12995282   427915261.8700  0.604500
        交易状态    滚动市盈率   市净率  滚动市销率    滚动市现率
        tradestatus      peTTM     pbMRQ     psTTM    pcfNcfTTM
                  1  27.432765  5.744737  2.540124  -150.111989
        """
        self.init()
        fields = "date,code,open,high,low,close,preclose,pctChg,volume,amount," \
                 "turn,tradestatus,peTTM,pbMRQ,psTTM,pcfNcfTTM"
        rs = bs.query_history_k_data_plus(code=ts_code,
                                          fields=fields,
                                          start_date=start_date, end_date=end_date,
                                          frequency="d", adjustflag="3")
        if rs.error_code != '0':
            printe(f'get_ts_code_daily fail, rs.error_code: {rs.error_code}')
            if rs.error_code == '10001001':
                bs.login()
            return self.query_history_k_data_plus(ts_code, start_date, end_date)

        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)
        # bs.logout()

        # 剔除停牌数据
        result = result[result['tradestatus'] == '1']
        result.drop(columns=['tradestatus'], inplace=True)
        result.rename(columns={'date': '日期',
                               'code': '证券代码',
                               'open': '开盘价',
                               'high': '最高价',
                               'low': '最低价',
                               'close': '收盘价',
                               'preclose': '前收盘价',
                               'pctChg': '涨跌幅',
                               'volume': '成交量',
                               'amount': '成交额',
                               'turn': '换手率',
                               'peTTM': '滚动市盈率',
                               'pbMRQ': '市净率',
                               'psTTM': '滚动市销率',
                               'pcfNcfTTM': '滚动市现率',
                               }, inplace=True)
        return result

    def query_dividend_data(self, ts_code_list, year):
        """
        获取某一年的所有个股分红信息
        :param ts_code_list:
        :param year:
        :return:
        """

        self.init()
        rs = None
        data_list = []
        for ts_code in ts_code_list:
            rs = bs.query_dividend_data(code=ts_code, year=year, yearType="operate")
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())

        df = pd.DataFrame(data_list, columns=rs.fields)
        df.rename(columns={
            'code': '证券代码',
            'dividPreNoticeDate': '预批露公告日',
            'dividAgmPumDate': '股东大会公告日期',
            'dividPlanAnnounceDate': '预案公告日',
            'dividPlanDate': '分红实施公告日',
            'dividRegistDate': '股权登记告日',
            'dividOperateDate': '除权除息日期',
            'dividPayDate': '派息日',
            'dividStockMarketDate': '红股上市交易日',
            'dividCashPsBeforeTax': '每股股利税前',
            'dividCashPsAfterTax': '每股股利税后',
            'dividStocksPs': '每股红股',
            'dividCashStock': '分红送转',
            'dividReserveToStockPs': '每股转增资本',
        }, inplace=True)
        df = self.correct_dividend_data(df)
        return df

    @staticmethod
    def correct_dividend_data(df):
        # 按指定字段去重, 保留最后一个
        df.drop_duplicates(subset=['证券代码', '除权除息日期'], keep='last', inplace=True)
        return df

    def query_profit_data(self):
        # todo

        # 查询季频估值指标盈利能力
        profit_list = []
        rs_profit = bs.query_profit_data(code="sh.600000", year=2017, quarter=2)
        while (rs_profit.error_code == '0') & rs_profit.next():
            profit_list.append(rs_profit.get_row_data())
        result_profit = pd.DataFrame(profit_list, columns=rs_profit.fields)
        return result_profit


if __name__ == '__main__':
    pd.pandas.set_option("display.width", 500)  # 数据显示区域的总宽度，以总字符数计算。
    pd.pandas.set_option("display.max_rows", None)  # 最大显示行数，超过该值用省略号代替，为None时显示所有行。
    pd.pandas.set_option("display.max_columns", None)  # 最大显示列数，超过该值用省略号代替，为None时显示所有列。
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)  # 使列名对齐
    pd.set_option('display.width', 100)  # 设置横向最多显示的字符
    pd.set_option('expand_frame_repr', False)  # 数据超过总宽度后，是否折叠显示
    pd.set_option('colheader_justify', 'right')

    from core.datas.ts_plot import ts_plot
    from_baostock = FromBaostock()
    # _df = from_baostock.get_ts_code_daily('sz.000001', '2018-01-01', '2021-05-25')
    # ts_plot.show_line(_df['收盘价'].astype(float),
    #                   _df['滚动市盈率'].astype(float),
    #                   _df['市净率'].astype(float),
    #                   )
    _df = from_baostock.query_dividend_data(["sh.600774"], '2018')
    print(_df)
