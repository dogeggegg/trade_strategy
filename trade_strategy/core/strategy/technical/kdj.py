# coding:utf-8

import numpy as np
from collections import deque
import copy


class KDJ:

    def generate_buy_mean_signal(self, ts_daily_df):





        return self.generate_buy_common_mean_signal(ts_daily_df, 'pct_chg_sum')

    def generate_sell_mean_signal(self, ts_daily_df):
        return self.generate_sell_common_mean_signal(ts_daily_df, 'pct_chg_sum')


kdj = KDJ()
