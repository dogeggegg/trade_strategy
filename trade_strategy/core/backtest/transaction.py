
import pandas as pd

from core.datas.ts_datas import ts_datas


class Transaction:

    # 当日交易记录，每日初始化置空
    cur_daily_trade_record_df = pd.DataFrame(columns=[
        '证券代码',
        '证券名称',
        '业务',
        '交易价格',
        '交易数量',
    ])

    def init_cur_daily_trade_record_df(self):
        """
        清空 当日交易记录
        :return:
        """
        self.cur_daily_trade_record_df.drop(self.cur_daily_trade_record_df.index, inplace=True)

    def get_cur_daily_trade_record_df(self):
        return self.cur_daily_trade_record_df

    def get_cur_daily_trade_record_stock_list(self):
        """
        获取当日交易记录的个股列表
        :return:
        """
        # 交易记录 需要按 证券代码 分开处理, 因为存在当日多次交易个股情况
        trade_record_stocks_list = self.cur_daily_trade_record_df[['证券代码', '证券名称']].values
        # 有序去重
        temp_list = list()
        for temp in trade_record_stocks_list:
            temp = list(temp)
            if temp not in temp_list:
                temp_list.append(temp)
        cur_daily_stock_list = temp_list
        return cur_daily_stock_list

    def get_cur_daily_trade_record_stock_df(self, ts_code):
        """
        获取当日交易记录中 个股的记录
        :param ts_code: 证券代码
        :return:
        """
        trading_record_df = self.cur_daily_trade_record_df[self.cur_daily_trade_record_df['证券代码'] == ts_code]
        return trading_record_df

    def add_cur_daily_trade_record(self, ts_code, ts_name, business, price, shares):
        """

        :param ts_code:
        :param ts_name:
        :param business:
        :param price:
        :param shares:
        :return:
        """
        self.cur_daily_trade_record_df.loc[len(self.cur_daily_trade_record_df)] = [
            ts_code, ts_name, business, price, shares
        ]

    # 交易每日变化记录
    trade_record_df = pd.DataFrame(columns=[
        '日期',
        '证券代码',
        '证券名称',
        '业务',
        '交易价格',
        '交易数量',
    ])

    def get_trade_record_df(self):
        """
        返回 交易记录
        :return:
        """
        return self.trade_record_df

    def add_trade_record(self, trade_date, ts_code, ts_name, business, price, shares):
        """
        添加 交易记录
        :param trade_date:
        :param ts_code:
        :param ts_name:
        :param business:
        :param price:
        :param shares:
        :return:
        """
        self.trade_record_df.loc[len(self.trade_record_df)] = [
            trade_date, ts_code, ts_name, business, price, shares]


transaction = Transaction()
