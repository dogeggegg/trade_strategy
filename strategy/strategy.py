# coding:utf-8

from strategy.mean import mean
from strategy.sort import sort

import pandas as pd
import numpy as np
from collections import deque


class Strategy:

    @staticmethod
    def generate_strategy_signal(ts_daily_df):
        mean_strategy_df = mean.generate_mean_signal(ts_daily_df, 'change_sum')
        strategy_df = mean_strategy_df
        strategy_df = strategy_df.sort_values(by='trade_date', ascending=True)
        strategy_df.index = range(len(strategy_df.index))
        return strategy_df


strategy = Strategy()
