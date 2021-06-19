
from logger.logger import *
from core.datas.ts_datas import ts_datas
from core.strategy.fundamental.fundamental import fundamental
from core.backtest.position import position
from config.config import UserConfig
from core.backtest.assets import assets


class Strategy(object):

    @staticmethod
    def strategy_fun(trade_date):
        """
       获取 trade_date 这一日排序股票
       :param trade_date: 日期
       :return:
       """
        # filter_stock_df = fundamental.sort_ascending_pe_ttm(trade_date)                 # 升序 滚动市盈率
        filter_stock_df = fundamental.sort_ascending_ps_ttm(trade_date, limit='0,10')     # 升序 滚动市销率 排序前十
        filter_stock_df.dropna(subset=['滚动市销率'], inplace=True)                      # 去除 nan
        filter_stock_df = filter_stock_df[~filter_stock_df['证券代码'].str.contains("ST")]                         # 去除 st股
        filter_stock_df.reset_index(inplace=True)
        if not filter_stock_df.empty:
            printf(f'{trade_date}:筛选股票为：\n{filter_stock_df}')
        cur_daily_position_df = position.get_cur_daily_position_df()
        # 卖出持仓中有，而筛选中没有的股票
        sell_stock_df = cur_daily_position_df[~cur_daily_position_df['证券代码'].isin(filter_stock_df['证券代码'])]
        # 买入筛选中有,而持仓中没有的股票
        buy_stock_df = filter_stock_df[~filter_stock_df['证券代码'].isin(cur_daily_position_df['证券代码'])]
        if not sell_stock_df.empty:
            printf(f'卖出:\n{sell_stock_df}')
        if not buy_stock_df.empty:
            printf(f'买入:\n{buy_stock_df}')
        return buy_stock_df, sell_stock_df

    @staticmethod
    def buy_strategy(trade_date, ts_code, ts_name):
        """
        输入该股，返回增持股数
        :param trade_date: 交易日
        :param ts_code: 证券代码
        :param ts_name: 证券名称
        :return:
        """
        df = ts_datas.get_ts_daily(trade_date, trade_date, ts_code)
        if df.empty:
            printe(f'{ts_code} {ts_name} 今天停牌啊，交易不了;')
            return 0, 0
        ts_code = df['证券代码'][0]
        ts_name = df['证券名称'][0]
        # 判断 持股数量
        total_assets = assets.get_total_assets()
        buy_amount = round(total_assets / UserConfig.HoldingSharesNumber, 2)  # 预期买入金额
        cash = assets.get_cur_cash()
        if cash < buy_amount:   # 现金不够，选择现金能买入的价格
            buy_amount = cash
        buy_price = float(df['最高价'][0])  # 去除择时，选最贵的买
        # 交易至多的股数， 新增持仓股数
        add_hold_share = int((buy_amount // (buy_price * 100)) * 100)
        if add_hold_share == 0:
            printe(f'这里连一手都买不起,{ts_code} {ts_name} buy_price:{buy_price}, buy_amount or cash:{buy_amount}')
            return 0, 0
        return buy_price, add_hold_share

    @staticmethod
    def sell_strategy(trade_date, ts_code, ts_name):
        """
        输入该股，返回减持股数
        :param trade_date: 交易日
        :param ts_code: 证券代码
        :param ts_name: 证券名称
        :return:
        """
        df = ts_datas.get_ts_daily(trade_date, trade_date, ts_code)
        if df.empty:
            printe(f'{ts_code} {ts_name} 今天停牌啊，交易不了;')
            return 0, 0
        sell_price = float(df['最低价'][0])  # 去除择时，选最便的买
        cur_holding_stock_df = position.get_cur_holding_stock_df(ts_code)
        hold_share = cur_holding_stock_df['持仓数量']
        if hold_share.empty:
            printe(f'都没有持仓该股:{ts_code}，卖个锤子')
            return 0, 0
        deduct_hold_share = hold_share
        return sell_price, deduct_hold_share.values[0]






