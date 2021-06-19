# coding:utf-8

from strategy.mean import mean
from strategy.sort import sort

import pandas as pd
import numpy as np
from collections import deque


class Action:

    def generate_action(self, strategy_df, ts_daily_df):
        strategy_ts_daily_df = ts_daily_df[ts_daily_df['trade_date'].isin(strategy_df['trade_date'])]
        strategy_next_day_df = ts_daily_df.loc[strategy_ts_daily_df.index + 1]
        strategy_next_day_df.index = range(len(strategy_next_day_df.index))
        action_df = strategy_next_day_df
        action_df['action'] = strategy_df['action']
        action_df['weight'] = strategy_df['weight']
        action_df['trade_price'] = action_df[['action', 'high', 'low']].apply(self.__calc_trade_price, axis=1)
        action_df = action_df[['ts_code', 'trade_date', 'trade_price', 'action', 'weight']]
        return action_df

    @staticmethod
    def __calc_trade_price(action_df):
        action_df_action = action_df['action']
        action_df_high = action_df['high']
        action_df_low = action_df['low']
        if 'buy' in action_df_action:
            price = action_df_high
        elif 'sell' in action_df_action:
            price = action_df_low
        else:
            raise Exception(f'未知的action:{action_df_action}')
        return price


action = Action()

