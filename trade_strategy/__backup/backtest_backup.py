# coding:utf-8

import pandas as pd
import numpy as np
import copy


class BackTest:

    total_amount = 100000
    commission_rate = 6 / 10000     # 佣金率
    account_df = pd.DataFrame(columns=['trade_date',        # 日期
                                       'ts_code',           # 代码
                                       'close',             # 收盘价
                                       'action',            # 动作
                                       'vol',               # 交易量
                                       'position',          # 持有股数
                                       'change_amount',     # 金额变动
                                       'fee',               # 手续费
                                       'cap',               # 持有股票市值
                                       'balance',           # 余额
                                       'assets'             # 总资产
                                       ])

    def get_account_df_last_position(self):
        if len(self.account_df):
            before_position = self.account_df.iloc[-1]['position']
        else:
            before_position = 0
        return before_position

    def get_account_df_last_balance(self):
        if len(self.account_df):
            before_balance = self.account_df.iloc[-1]['balance']
        else:
            before_balance = self.total_amount
        return before_balance

    def append_account_df(self,
                          ts_code,
                          trade_date,
                          close,
                          action,
                          vol,
                          position,
                          change_amount,
                          fee,
                          cap,
                          balance,
                          assets):
        self.account_df.loc[len(self.account_df)] = [ts_code,           # 代码
                                                     trade_date,        # 日期
                                                     close,             # 收盘价
                                                     action,            # 动作
                                                     vol,               # 交易量
                                                     position,          # 持有股数
                                                     change_amount,     # 金额变动
                                                     fee,               # 手续费
                                                     cap,               # 持有股票市值
                                                     balance,           # 余额
                                                     assets             # 总资产
                                                     ]

    def calc_earn_rate(self, action_df, ts_daily_df):
        """
        回测,计算收益率
        :param action_df: 策略的DataFrame
        :param ts_daily_df: 日线行情
        :return:
        """
        ts_daily_df = pd.merge(ts_daily_df, action_df, on=['trade_date', 'ts_code'], how='outer')
        ts_daily_df.loc[ts_daily_df['action'].isna(), 'action'] = ''
        cols = ['ts_code', 'trade_date', 'close', 'action', 'trade_price', 'weight']
        # ts_daily_df[cols].apply(self.__apply_daily_return, axis=1)
        for index, df_row in ts_daily_df[cols].iterrows():
            self.__apply_daily_return(df_row)
        account_df = self.account_df
        return account_df

    def __apply_daily_return(self, daily_df):
        ts_code = daily_df['ts_code']
        trade_date = daily_df['trade_date']
        close_price = daily_df['close']
        action = daily_df['action']
        trade_price = daily_df['trade_price']
        weight = daily_df['weight']

        # 交易前持仓股数
        before_position = self.get_account_df_last_position()
        # 交易前余额
        before_balance = self.get_account_df_last_balance()
        # 每次交易至多交易10%的总资产
        preset_position = 0.1
        most_amount = self.total_amount * preset_position

        if 'buy' in action:   # 买入
            (trade_vol, position,
             change_amount, fee,
             balance) = self.buy_action(before_position,
                                        before_balance,
                                        ts_code,
                                        trade_price,
                                        weight,
                                        most_amount)
        elif 'sell' in action:  # 卖出
            (trade_vol, position,
             change_amount, fee,
             balance) = self.sell_action(before_position,
                                         before_balance,
                                         ts_code,
                                         trade_price,
                                         weight,
                                         most_amount)
        else:   # 无交易行为
            trade_vol = 0
            position = before_position
            change_amount = 0
            fee = 0
            balance = before_balance

        # 市值 = 价格 * 持有股数
        cap = close_price * position
        # 总资产 = 市值(价格 * 持有股数) + 余额
        assets = cap + balance
        self.append_account_df(ts_code,
                               trade_date,
                               close_price,
                               action,
                               trade_vol,
                               position,
                               change_amount,
                               fee,
                               cap,
                               balance,
                               assets)
        return 0

    def calc_fee(self, ts_code, action, transaction_amount):
        """
        :param ts_code: 股票代码
        :param action:  交易动作
        :param transaction_amount:  交易金额
        :return:
        """
        # 手续费 券商收取 双向收取
        commission = round(transaction_amount * self.commission_rate, 2)
        commission = 5 if 0 < commission < 5 else commission
        # 过户费 沪股 双向收取
        if ts_code[-2] == 'SH':
            transfer_fee = round(transaction_amount * 0.00002, 2)
        else:
            transfer_fee = 0
        if 'sell' in action:
            # 印花税 卖出时收取
            stamp_duty = round(transaction_amount * 0.001, 2)
        else:
            stamp_duty = 0
        fee = commission + transfer_fee + stamp_duty
        return fee

    def buy_action(self,
                   before_position,
                   before_balance,
                   ts_code,
                   trade_price,
                   weight,
                   most_amount):
        """
        执行买入操作
        :param before_position: 交易前持仓
        :param before_balance: 交易前余额
        :param ts_code: 股票代码
        :param trade_price: 交易价格
        :param weight: 权重
        :param most_amount: 单次交易上限金额
        :return: shares：交易股数
                 after_position  交易后持仓
                 change_amount 变化金额
                 extra_costs 额外费用
                 after_balance  交易后余额
        """
        # 若可用余额比预期交易金额少，则按可用余额计算
        if before_balance < most_amount:
            most_amount = before_balance
        # 交易至多的股数
        most_trade_vol = (most_amount // (trade_price * 100)) * 100
        # 计算权重 买入股数, 若能买入，至少100股
        trade_vol = ((most_trade_vol * weight) // 100 * 100) or 100 if most_trade_vol else 0
        # 交易后持仓股数
        position = before_position + trade_vol
        # 交易金额
        transaction_amount = trade_price * trade_vol
        # 计算额外费用，如 手续费 过户费 印花税
        fee = self.calc_fee(ts_code, 'buy', transaction_amount)
        # 变化金额
        change_amount = - transaction_amount - fee
        # 交易后余额
        balance = before_balance + change_amount
        return trade_vol, position, change_amount, fee, balance

    def sell_action(self,
                    before_position,
                    before_balance,
                    ts_code,
                    trade_price,
                    weight,
                    most_amount):
        """
        执行卖出操作
        :param before_position: 交易前持仓
        :param before_balance: 交易前余额
        :param ts_code: 股票代码
        :param trade_price: 交易价格
        :param weight: 权重
        :param most_amount: 单次交易上限金额
        :return: shares：交易股数
                 after_position  交易后持仓
                 change_amount 变化金额
                 extra_costs 额外费用
                 after_balance  交易后余额
        """
        # 若可用市值比预期交易金额少，则按可用市值计算
        cap = before_position * trade_price
        if cap < most_amount:
            most_amount = cap
        # 交易至多的股数
        most_trade_vol = (most_amount // (trade_price * 100)) * 100
        # 计算权重 卖出股数， 若能卖出，至少100股
        trade_vol = ((most_trade_vol * weight) // 100 * 100) or 100 if most_trade_vol else 0
        # 交易后持仓股数
        position = before_position - trade_vol
        # 交易金额
        transaction_amount = trade_price * trade_vol
        # 计算额外费用，如 手续费 过户费 印花税
        fee = self.calc_fee(ts_code, 'sell', transaction_amount)
        # 变化金额
        change_amount = transaction_amount - fee
        # 交易后余额
        balance = before_balance + change_amount
        return trade_vol, position, change_amount, fee, balance


backtest = BackTest()
