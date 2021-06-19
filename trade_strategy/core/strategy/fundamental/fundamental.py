import pandas as pd
from interval import Interval
import matplotlib.pyplot as plt
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta


from logger.logger import *
from config.config import Config
from core.datas.ts_datas import ts_datas
from core.datas.ts_plot import ts_plot


class Fundamental(object):

    @staticmethod
    def sort_ascending_pe_ttm(trade_date, limit=None):
        """
        升序排序 滚动市盈率，越小越好
        :param trade_date:
        :param limit:
        :return:
        """
        df = ts_datas.get_ts_daily(trade_date, trade_date,
                                   filters=('ST', False),  # 去除ST
                                   order=('滚动市盈率', 'DESC'),
                                   limit=limit)
        # df = df.sort_values('滚动市盈率', ascending=True)  # ascending 升序
        return df[['证券代码', '证券名称', '滚动市盈率', '收盘价']]

    @staticmethod
    def sort_ascending_ps_ttm(trade_date, limit=None):
        """
        升序排序 滚动市销率，越小越好
        :param trade_date:
        :param limit:
        :return:
        """
        df = ts_datas.get_ts_daily(trade_date, trade_date,
                                   filters=('ST', False),           # 去除ST
                                   order=('滚动市销率', 'DESC'),     # 降序
                                   limit=limit)
        # df = df.sort_values('滚动市销率', ascending=True)  # ascending 升序
        return df[['证券代码', '证券名称', '滚动市销率', '收盘价']]

    @staticmethod
    def get_change_df(start_date, end_date):
        df = ts_datas.get_ts_daily(start_date, end_date)
        ts_code_df = df[['证券代码', '证券名称']].drop_duplicates()
        change_df = pd.DataFrame(columns=['证券代码', '证券名称', '所属行业', '收盘价', '涨跌额', '涨跌幅', '总市值'])
        for index, row in ts_code_df.iterrows():
            ts_code = row['证券代码']
            ts_name = row['证券名称']
            stock_df = df[df['证券代码'] == ts_code]
            if len(stock_df) <= 15:  # 小于10个交易日的新股 剔除
                continue
            change = round(stock_df['涨跌额'].sum(), 2)
            pre_close = stock_df['前收盘价'].head(1).values[0]
            # close = stock_df['收盘价'].tail(1).values[0]
            close = pre_close + change
            pct_change = round((close - pre_close) / pre_close, 5)
            total_mv = stock_df['总市值'].tail(1).values[0]
            industry = stock_df['所属行业'].values[0]
            change_df.loc[len(change_df)] = [ts_code, ts_name, industry, close, change, pct_change, total_mv]
        change_df = change_df[~change_df['证券代码'].str.contains('sh.68')]
        change_df = change_df[~change_df['证券代码'].str.contains('sz.30')]
        change_df = change_df[~change_df['证券名称'].str.contains('ST')]

        def total_mv_map(x):
            x = x / 10000
            step = 100000 // 10000
            n = 1
            while 1:
                if x < n * step:
                    return (n - 1) * step
                n += 1

        change_df['总市值分段'] = change_df['总市值'].map(total_mv_map)
        change_df.sort_values(['涨跌幅', '涨跌额'], ascending=[False, False], inplace=True)
        change_df.reset_index(drop=True, inplace=True)
        return change_df

    @staticmethod
    def sort_total_mv(change_df):
        total_mv_threshold = 30000000
        change_df.sort_values(['总市值分段'], ascending=[False], inplace=True)
        change_df.reset_index(drop=True, inplace=True)
        change_df = change_df[change_df['总市值'] >= total_mv_threshold]
        change_df = change_df[change_df['所属行业'] != '银行']
        change_df.reset_index(drop=True, inplace=True)
        return change_df

    @staticmethod
    def show_total_mv_threshold(change_df):
        change_up10_df = change_df[change_df['涨跌幅'] > 0.1]
        change_down10_df = change_df[change_df['涨跌幅'] < -0.1]

        up10_threshold_list = list()
        down10_threshold_list = list()
        for threshold in change_df['总市值分段'].drop_duplicates():
            total_num = len(change_df[change_df['总市值分段'] >= threshold])
            up10_num = len(change_up10_df[change_up10_df['总市值分段'] >= threshold])
            down10_num = len(change_down10_df[change_down10_df['总市值分段'] >= threshold])
            up_10_threshold_percent = up10_num / total_num
            down10_threshold_percent = down10_num / total_num
            up10_threshold_list.append((threshold, up_10_threshold_percent))
            down10_threshold_list.append((threshold, down10_threshold_percent))

        def key_fun(x):
            return x[1]

        up10_max = max(up10_threshold_list, key=key_fun)
        down10_max = max(down10_threshold_list, key=key_fun)
        down10_min = min(down10_threshold_list, key=key_fun)
        printf(f'up10_max:{up10_max}, down10_max:{down10_max}, down10_min:{down10_min}')

        up10_threshold_df = pd.DataFrame(up10_threshold_list, columns=['总市值', '+10%概率'])
        up10_threshold_df.sort_values('总市值', inplace=True)
        up10_threshold_df = up10_threshold_df[up10_threshold_df['+10%概率'] != 0]

        down10_threshold_df = pd.DataFrame(down10_threshold_list, columns=['总市值', '-10%概率'])
        down10_threshold_df.sort_values('总市值', inplace=True)
        down10_threshold_df = down10_threshold_df[down10_threshold_df['-10%概率'] != 0]

        threshold_df = pd.merge(up10_threshold_df, down10_threshold_df, on='总市值', how='outer')
        threshold_df.replace(np.nan, 0, inplace=True)
        threshold_df['合计'] = threshold_df['+10%概率'] - threshold_df['-10%概率']
        ts_plot.show_line(threshold_df, x_label='总市值')



    # def sort_roe(self, date):
    #     pass
    #
    # @staticmethod
    # def sort_descending_dv_ttm(date):
    #     """
    #     降序排序 股息率，股息率是股息与股票价格之间的比率，越大越好
    #     :param date:
    #     :return:
    #     """
    #     df = ts_requests.get_ts_date_daily_basic(date)
    #     df = df.sort_values('dv_ttm', ascending=False)  # descending 降序
    #     return df[['ts_code', 'dv_ttm']]
    #
    # @staticmethod
    # def sort_descending_circ_mv(date):
    #     """
    #     降序排序 流通市值，越大越好
    #     :param date:
    #     :return:
    #     """
    #     df = ts_requests.get_ts_date_daily_basic(date)
    #     df = df.sort_values('circ_mv', ascending=False)  # descending 降序
    #     return df[['ts_code', 'circ_mv']]

fundamental = Fundamental()


if __name__ == '__main__':
    logger.add_file_handler(Config.LogFilePath)
    start_date = (datetime.datetime.now() - relativedelta(months=1)).strftime('%Y-%m-%d')
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    start_date = '2021-05-10'
    end_date = '2021-06-10'
    _change_df = fundamental.get_change_df(start_date, end_date)
    fundamental.show_total_mv_threshold(_change_df)
    _change_df = fundamental.sort_total_mv(_change_df)
    print(_change_df)


