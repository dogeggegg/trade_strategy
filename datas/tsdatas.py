# coding:utf-8

import pandas as pd
import tushare as ts

pd.pandas.set_option("display.width", 500)              # 数据显示区域的总宽度，以总字符数计算。
pd.pandas.set_option("display.max_rows", None)        # 最大显示行数，超过该值用省略号代替，为None时显示所有行。
pd.pandas.set_option("display.max_columns", None)       # 最大显示列数，超过该值用省略号代替，为None时显示所有列。


class TsDatas(object):

    ts.set_token('54f042d586a61d1911fe7a8fd4f0e8324995eb37f157af0300e24cd2')
    pro = ts.pro_api(timeout=5)

    def get_all_ts_code(self):
        """
        返回所有股票代码
        :return:  pandas / list
        """
        df = self.pro.query('stock_basic',
                            exchange='',        # 交易所 SSE 上交所 SZSE 深交所 HKEX 港交所
                            list_status='L',    # 上市状态 L上市 D退市 P暂停上市
                            fields='ts_code,symbol,name,area,industry,list_date')
        #         ts_code
        # 0     000001.SZ
        # ...         ...
        # 3918  688981.SH
        return df

    def get_ts_daily(self, ts_code, start_date=None):
        """
        返回指定股票的参数: ts_code、trade_date、open、high、low、close、pre_close、change、pct_chg、vol、amount
        :param ts_code:     "000001.SZ"
        :param start_date:  "20200804"
        # :param end_date:    "20200804"
        :return:            pandas
     股票代码       日期  开盘价  最高价 最低价  收盘价  昨日收盘价  涨跌额    涨跌幅      成交量       成交额
     ts_code trade_date  open   high    low  close  pre_close  change  pct_chg        vol      amount
0  002120.SZ   20200804  24.6  24.65  24.01  24.11      24.43   -0.32  -1.3099  128651.79  311500.045
        """

        df = self.pro.daily('daily', ts_code=ts_code, start_date=start_date)
        df = df.sort_values(by='trade_date')
        df.index = range(len(df.index))
        change_sum_list = list()
        change_sum = 0
        for index, row in df.iterrows():
            change_sum += row['change']
            change_sum_list.append(round(change_sum, 3))
        df['change_sum'] = change_sum_list
        return df

    def get_ts_dividend(self, ts_code, start_date):
        """
        todo: 返回分红数据
        :param ts_code:
        :param start_date:
        :return:
        """
        df = self.pro.dividend(ts_code=ts_code, fields='ts_code,div_proc,stk_div,record_date,ex_date')
        return df


tsdatas = TsDatas()


if __name__ == '__main__':
    # print(tsdatas.get_ts_daily("002120.SZ", "20190225"))
    print(tsdatas.get_all_ts_code())
