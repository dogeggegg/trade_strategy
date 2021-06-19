# coding:utf-8

import numpy as np
from collections import deque
import copy
import pandas as pd
import pprint


class Mean:

    mean_signal_list = list()

    def generate_mean_signal(self, ts_daily_df, ma_type):
        """
        :param ts_daily_df: 日线
        :param ma_type: 均线类型: open high low close pre_close change pct_chg vol amount pct_chg_sum
        :return: list 买入信号
        """
        ma_list = [5, 10, 15, 20, 25]
        ma_df, ma_type_list = self.__generate_ma(ts_daily_df, ma_type, ma_list)
        mean_trend_df = ma_df.rolling(5).apply(self.__apply_mean_trend, raw=True)
        mean_trend_df.insert(0, 'trade_date', ts_daily_df['trade_date'])
        mean_trend_df.insert(0, 'ts_code', ts_daily_df['ts_code'])
        buy_df = mean_trend_df.copy(deep=True)
        # 获取mean_trend_df 中所有趋势向下的值
        for _ma_type in ma_type_list:
            buy_df = buy_df[buy_df[_ma_type] == -1]     # trend down
        # 获取所有趋势向下的 ts_daily_df、ma_df， 按 日期 合并
        buy_df = pd.merge(ts_daily_df.loc[buy_df.index], ma_df.loc[buy_df.index], on=['trade_date', 'ts_code'])
        # 获取形成向上突破均线的df
        buy_df = buy_df[buy_df[ma_type] > buy_df[ma_type_list[0]]]
        buy_df['action'] = ['buy_mean'] * len(buy_df)
        buy_df['weight'] = buy_df[['open', 'close', 'high']].apply(self.__apply_calc_buy_weight, axis=1)
        buy_df = buy_df[buy_df['weight'] > 0]

        sell_df = mean_trend_df.copy(deep=True)
        for _ma_type in ma_type_list:
            sell_df = sell_df[sell_df[_ma_type] == 1]   # trend up
        # 获取所有趋势向上的 ts_daily_df、ma_df， 按 日期 合并
        sell_df = pd.merge(ts_daily_df.loc[sell_df.index], ma_df.loc[sell_df.index], on=['trade_date', 'ts_code'])
        # 获取形成向下突破均线的df
        sell_df = sell_df[sell_df[ma_type] < sell_df[ma_type_list[0]]]
        sell_df['action'] = ['sell_mean'] * len(sell_df)
        sell_df['weight'] = sell_df[['open', 'close', 'low']].apply(self.__apply_calc_sell_weight, axis=1)
        sell_df = sell_df[sell_df['weight'] > 0]

        mean_strategy_df = pd.concat([buy_df, sell_df])
        mean_strategy_df.index = range(len(mean_strategy_df.index))
        mean_strategy_df = mean_strategy_df[['ts_code', 'trade_date', 'action', 'weight']]
        return mean_strategy_df

    @staticmethod
    def __generate_ma(ts_daily_df, ma_type, ma_list):
        """
        生产 x日 均线
        :param ts_daily_df: 日线行情
        :param ma_type:均线类型: open high low close pre_close change pct_chg vol amount pct_chg_sum
        :param ma_list:  [5, 10, 20, 30]    # 生成 5 10 20 30 日均线
        :return: DataFrame
        """
        assert ma_type in ts_daily_df.columns, '不支持的 ma_type:%s' % ma_type
        data = ts_daily_df[ma_type]
        ma_type_list = list()
        ma_df = pd.DataFrame()
        ma_df['ts_code'] = ts_daily_df['ts_code']
        ma_df['trade_date'] = ts_daily_df['trade_date']
        for ma in ma_list:
            _ma_type = f'{ma_type}_ma{ma}'
            ma_df[_ma_type] = data.rolling(ma).mean()
            ma_type_list.append(_ma_type)
        return ma_df, ma_type_list

    def __apply_mean_trend(self, data_list):
        if self.__judge_trend_up(data_list):
            return 1    # trend up
        elif self.__judge_trend_down(data_list):
            return -1   # trend down
        else:
            return 0    # trend balance

    @staticmethod
    def __judge_trend_up(data_list):
        for x, y in zip(data_list, data_list[1:]):
            if x > y:
                return False
        return True

    @staticmethod
    def __judge_trend_down(data_list):
        for x, y in zip(data_list, data_list[1:]):
            if x < y:
                return False
        return True

    @staticmethod
    def __apply_calc_buy_weight(buy_df):
        buy_df_open = buy_df['open']
        buy_df_close = buy_df['close']
        buy_df_high = buy_df['high']
        numerator = buy_df_close - buy_df_open
        denominator = buy_df_high - buy_df_open
        if numerator > 0 and denominator > 0:
            weight = round(numerator / denominator, 2)
        else:
            weight = 0
        return weight

    @staticmethod
    def __apply_calc_sell_weight(sell_df):
        sell_df_open = sell_df['open']
        sell_df_close = sell_df['close']
        sell_df_low = sell_df['low']
        numerator = sell_df_open - sell_df_close
        denominator = sell_df_open - sell_df_low
        if numerator > 0 and denominator > 0:
            weight = round(numerator / denominator, 2)
        else:
            weight = 0
        return weight


mean = Mean()
