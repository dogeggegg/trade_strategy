# coding:utf-8

from datas.tsdatas import tsdatas
from strategy.strategy import strategy
from action.action import action
from backtest.backtest import backtest

import datetime

date_object = datetime.datetime.now() - datetime.timedelta(days=3*365)
longlong_ago = date_object.strftime("%Y%m%d")
ts_daily_df = tsdatas.get_ts_daily("002120.SZ", "20170725")
print(ts_daily_df)
strategy_df = strategy.generate_strategy_signal(ts_daily_df)
print(strategy_df)
action_df = action.generate_action(strategy_df, ts_daily_df)
print(action_df)
account_df = backtest.calc_earn_rate(action_df, ts_daily_df)
print(account_df)





