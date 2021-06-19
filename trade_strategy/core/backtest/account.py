import pandas as pd
import pprint

from logger.logger import *
from config.config import UserConfig
from core.datas.ts_datas import ts_datas
from core.backtest.transaction import transaction
from core.backtest.position import position
from core.backtest.assets import assets


class Account:

    @staticmethod
    def init_account_daily(trade_date):
        """
        每日初始化 当日账户情况, 如 置空当日交易记录, 更新当日持仓
        :return:
        """
        transaction.init_cur_daily_trade_record_df()
        position.init_cur_daily_position_df(trade_date)

    @staticmethod
    def settle_account_daily(trade_date):
        """
        结算 当日账户情况, 计算 持仓占比 等
        :return:
        """
        cash = assets.get_cur_cash()
        # 获取持仓总市值
        total_cap = position.get_cur_holding_stock_col_sum('持仓市值')
        # 获取持仓总持仓盈亏
        profit_loss = position.get_cur_holding_stock_col_sum('持仓盈亏')
        # 获取持仓总今日盈亏
        profit_loss_today = position.get_cur_holding_stock_col_sum('今日盈亏')
        # 总资产 = 持仓市值求和 + 可用现金
        total_assets = total_cap + cash

        cur_daily_position_df = position.get_cur_daily_position_df()
        for index, row in cur_daily_position_df.iterrows():
            ts_code = row['证券代码']
            cap = row['持仓市值']
            proportion = round(cap / total_assets, 2)                   # 持仓占比 = 持仓市值 / 总资产
            position.set_cur_daily_position_df(ts_code, '持仓占比', proportion)

        cur_daily_position_df = position.get_cur_daily_position_df()
        position.add_positions_df(cur_daily_position_df)
        account_record = (trade_date, total_assets, total_cap,
                          profit_loss, profit_loss_today, cash)
        assets.append_account_df(*account_record)
        printf(f'当前持仓状态:\n{cur_daily_position_df}')
        account_df = pd.DataFrame({
            '日期': [trade_date],
            '总资产': [total_assets],
            '持仓市值': [total_cap],
            '持仓盈亏': [profit_loss],
            '今日盈亏': [profit_loss_today],
            '可用现金': [cash]
        })
        printf(f'当前账户状态:\n{pprint.pformat(account_df)}')

    @staticmethod
    def add_trade_record(trade_date, business, ts_code, ts_name, price, shares):
        if shares == 0:
            return
        assert business in ['买入', '卖出'], f'这是什么业务? {business}??'
        transaction.add_cur_daily_trade_record(
            ts_code, ts_name, business, price, shares
        )
        transaction.add_trade_record(
            trade_date, ts_code, ts_name, business, price, shares)

    def deal_trade_records(self, trade_date):
        """
        处理 交易记录, 计算出 持仓 成本、今日盈亏
        :param trade_date: 日期
        :return:
        """
        # 获取当日交易记录的个股列表
        cur_daily_stock_list = transaction.get_cur_daily_trade_record_stock_list()
        for ts_code, ts_name in cur_daily_stock_list:
            # 交易记录中，按个股分组，每个个股的交易情况，比如:存在一个个股当日买卖多次的情况
            trade_record_df = transaction.get_cur_daily_trade_record_stock_df(ts_code)
            df = ts_datas.get_ts_daily(trade_date, trade_date, ts_code)

            close = float(df['收盘价'].values[0])
            pre_close = float(df['前收盘价'].values[0])

            for index, row in trade_record_df.iterrows():
                ts_name = row['证券名称']
                business = row['业务']
                price = row['交易价格']
                shares = row['交易数量']
                self.deal_trade_stock(trade_date, ts_code, ts_name, business, price, shares, close, pre_close)

    def deal_trade_stock(self, trade_date, ts_code, ts_name, business, price, shares, close, pre_close):
        """
        将 一条 交易记录处理完，反馈到 当日持仓中
        :param trade_date:
        :param ts_code:
        :param ts_name:
        :param business:
        :param price:
        :param shares:
        :param close:
        :param pre_close:
        :return:
        """
        holding_stock_df = position.get_cur_holding_stock_df(ts_code)
        # 获取持仓该个股的情况
        if holding_stock_df.empty:  # 之前未持仓
            position.add_cur_daily_position_df(trade_date, ts_code, ts_name, 0, 0, 0, 0, 0, 0, 0, 0)
            holding_stock_df = position.get_cur_holding_stock_df(ts_code)

        pre_cost = float(holding_stock_df['成本'].values[0])                    # 原成本
        pre_hold_share = float(holding_stock_df['持仓数量'].values[0])          # 原持仓数量
        pre_available_share = float(holding_stock_df['可用数量'].values[0])     # 原可用数量
        pre_profit_loss = float(holding_stock_df['持仓盈亏'].values[0])         # 原持仓盈亏
        pre_profit_loss_today = float(holding_stock_df['持仓盈亏'].values[0])   # 原持仓盈亏

        if business == '买入':
            result = self.buy_business(pre_hold_share, pre_available_share, pre_cost, pre_profit_loss_today,
                                       shares, price, ts_code, close)
            if not result:
                return
            hold_share, available_share, cost, profit_loss_today = result
        else:
            result = self.sell_business(pre_hold_share, pre_available_share, pre_cost, pre_profit_loss_today,
                                        shares, price, ts_code, pre_close)
            if not result:
                return
            hold_share, available_share, cost, profit_loss_today = result

        cap = close * hold_share                            # 持仓市值 = 现价*持仓数量
        position.set_cur_daily_position_df(ts_code, '持仓市值', cap)

        profit_loss = pre_profit_loss + profit_loss_today           # 持仓盈亏 = 原持仓盈亏 + 今日盈亏
        position.set_cur_daily_position_df(ts_code, '持仓数量', hold_share)
        position.set_cur_daily_position_df(ts_code, '可用数量', available_share)
        position.set_cur_daily_position_df(ts_code, '现价', close)
        position.set_cur_daily_position_df(ts_code, '成本', cost)
        position.set_cur_daily_position_df(ts_code, '持仓盈亏', profit_loss)
        position.set_cur_daily_position_df(ts_code, '今日盈亏', profit_loss_today)

    def buy_business(self, pre_hold_share, pre_available_share, pre_cost, pre_profit_loss_today,
                     shares, price, ts_code, close):
        share_change = shares
        hold_share = pre_hold_share + share_change
        available_share = pre_available_share                   # 买入，可用数量 = 原可用数量
        transaction_amount = price * share_change
        fee = self.calc_fee(ts_code, '买入', transaction_amount)
        # 成本 = (原成本*原持仓数量+该股流入流出资金+税)/持仓数量
        cost = (pre_cost * pre_hold_share + transaction_amount + fee) / hold_share
        trade_record_profit_loss_today = (close - price) * shares       # 该条记录的盈亏 = (收盘价 - 买入价) * 加仓数量
        profit_loss_today = pre_profit_loss_today + trade_record_profit_loss_today
        pre_cash = assets.get_cur_cash()
        if pre_cash - transaction_amount - fee < 0:
            printe(f'这里连税都交不起,pre_cash:{pre_cash},'
                   f'transaction_amount:{transaction_amount},'
                   f'price:{price},'
                   f'shares:{shares},'
                   f'fee:{fee}')
            return
        cash = pre_cash - transaction_amount - fee
        assets.set_cur_cash(cash)
        # 返回  (持仓数量, 可用数量, 成本, 今日盈亏)
        result = tuple(map(lambda x: round(x, 2), (hold_share, available_share, cost, profit_loss_today)))
        return result

    def sell_business(self, pre_hold_share, pre_available_share, pre_cost, pre_profit_loss_today,
                      shares, price, ts_code, pre_close):
        share_change = -shares                                  # 卖出股数需要减少，应该是负数
        hold_share = pre_hold_share + share_change
        available_share = pre_available_share + share_change    # 卖出，可用数量 = 原可用数量 - 卖出股数
        if available_share < 0:
            printe(f'原有可用股数:{pre_available_share}, share_change:{share_change}, 不够用啊')
            return
        transaction_amount = price * share_change               # 卖出为流出资金，应该是负数
        fee = self.calc_fee(ts_code, '卖出', transaction_amount)
        # 成本 = (原成本*原持仓数量+该股流入流出资金+税)/持仓数量
        if hold_share == 0:
            cost = 0    # 全卖出的情况下，无持股，无成本
        else:
            cost = (pre_cost * pre_hold_share + transaction_amount + fee) / hold_share
        trade_record_profit_loss_today = (price - pre_close) * shares   # 该条记录的盈亏 = (卖出价 - 前收盘价) * 减仓数量
        profit_loss_today = pre_profit_loss_today + trade_record_profit_loss_today
        pre_cash = assets.get_cur_cash()
        cash = pre_cash - transaction_amount - fee              # transaction_amount 为负数
        assets.set_cur_cash(cash)
        # 返回  (持仓数量, 可用数量, 成本, 今日盈亏)
        result = tuple(map(lambda x: round(x, 2), (hold_share, available_share, cost, profit_loss_today)))
        return result

    @staticmethod
    def deal_holding_stocks(trade_date):
        """
        处理 持仓个股
        :param trade_date:
        :return:
        """
        # 交易记录的个股
        cur_daily_stock_list = transaction.get_cur_daily_trade_record_stock_list()
        # 当前持仓状态
        cur_daily_position_df = position.get_cur_daily_position_df()
        # 更新处理未在交易记录的持仓
        for index, row in cur_daily_position_df.iterrows():
            # 以下变量是不会变的
            ts_code = row['证券代码']
            ts_name = row['证券名称']
            if [ts_code, ts_name] in cur_daily_stock_list:  # 交易记录内，已处理
                continue
            # 以下变量无需更新
            # cost = row['成本']                      # 只有买卖的时候变化
            hold_share = row['持仓数量']            # 只有买卖的时候变化
            # available_share = row['可用数量']       # 只有买卖的时候变化

            # 以下变量每日更新
            df = ts_datas.get_ts_daily(trade_date, trade_date, ts_code)
            if df.empty:
                ts_name = row['证券名称']
                printe(f'{ts_code} {ts_name} 今天停牌了,持仓个股不更新')
                continue
            pre_close = float(df['前收盘价'].values[0])
            close = float(df['收盘价'].values[0])

            pre_profit_loss = row['持仓盈亏']
            profit_loss_today = round((close - pre_close) * hold_share, 2)  # (收盘价 - 前收盘价) * 一直持仓数量

            cap = round(close * hold_share, 2)  # 持仓市值 = 现价*持仓数量
            position.set_cur_daily_position_df(ts_code, '持仓市值', cap)

            profit_loss = round(pre_profit_loss + profit_loss_today, 2)           # 持仓盈亏 = 原持仓盈亏 + 今日盈亏

            # position.set_cur_daily_position_df(ts_code, '持仓数量', hold_share)
            # position.set_cur_daily_position_df(ts_code, '可用数量', available_share)
            position.set_cur_daily_position_df(ts_code, '现价', close)
            # position.set_cur_daily_position_df(ts_code, '成本', cost)
            position.set_cur_daily_position_df(ts_code, '持仓盈亏', profit_loss)
            position.set_cur_daily_position_df(ts_code, '今日盈亏', profit_loss_today)

    @staticmethod
    def calc_fee(ts_code, business, transaction_amount):
        """
        计算 需要扣除的 (佣金 + 过户费 + 印花税）
        :param ts_code: 股票代码
        :param business:  业务: 买入/卖出
        :param transaction_amount:  交易金额
        :return:
        """
        # 手续费 券商收取 双向收取 不足5元按5元收
        commission = round(transaction_amount * UserConfig.CommissionRate, 2)
        commission = 5 if 0 < commission < 5 else commission
        # 过户费 沪股 双向收取
        if 'sh' in ts_code:
            transfer_fee = round(transaction_amount * 0.00002, 2)
        else:
            transfer_fee = 0
        if business == '卖出':
            # 印花税 卖出时收取
            stamp_duty = round(transaction_amount * 0.001, 2)
        else:   # 买入
            stamp_duty = 0
        fee = commission + transfer_fee + stamp_duty
        return fee


account = Account()
