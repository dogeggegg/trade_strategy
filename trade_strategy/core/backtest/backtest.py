# coding:utf-8


from logger.logger import *
from config.config import UserConfig
from core.datas.ts_datas import ts_datas

from core.backtest.account import account


class BackTest:

    @staticmethod
    def run_backtest(strategy):
        """
        开始回测, 返回一个回测详情 df
        :param strategy: 策略
        :return:
        """
        # 初始化数据库、配置
        ts_datas.init_data_table_config()
        # 获取回测区间
        start = UserConfig.Start
        end = UserConfig.End
        printf(f'回测开始，回测区间: start:{start}, end:{end}')
        # 更新数据库
        ts_datas.update_date(start, end)
        # 获取交易日历
        trade_cal_list = ts_datas.get_trade_cal(start, end)
        for trade_date in trade_cal_list:
            printw(f'回测日期:{trade_date}')
            # 初始化 每日账户情况
            account.init_account_daily(trade_date)
            # 选股策略
            buy_stock_df, sell_stock_df = strategy.strategy_fun(trade_date)
            # 先卖了才有钱买
            for ts_code, ts_name in sell_stock_df[['证券代码', '证券名称']].values:
                # 卖出策略
                sell_price, deduct_hold_share = strategy.sell_strategy(trade_date, ts_code, ts_name)
                # 添加 卖出 成交记录
                account.add_trade_record(trade_date, '卖出',
                                         ts_code, ts_name,
                                         sell_price, deduct_hold_share)
            for ts_code, ts_name in buy_stock_df[['证券代码', '证券名称']].values:
                # 买入策略
                buy_price, add_hold_share = strategy.buy_strategy(trade_date, ts_code, ts_name)
                # 添加 买入 成交记录
                account.add_trade_record(trade_date, '买入',
                                         ts_code, ts_name,
                                         buy_price, add_hold_share)

            # 处理交易记录
            account.deal_trade_records(trade_date)
            # 更新持仓记录
            account.deal_holding_stocks(trade_date)
            # 统计 每日账户情况
            account.settle_account_daily(trade_date)


backtest = BackTest()
