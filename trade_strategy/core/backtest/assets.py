import pandas as pd

from config.config import UserConfig


class Assets:
    # 可用现金变量
    cash = UserConfig.CapitalBase

    def get_cur_cash(self):
        """
        获取当前可用现金
        :return:
        """
        return self.cash

    def set_cur_cash(self, cash):
        """
        设置当前可用资金
        :param cash:
        :return:
        """
        self.cash = round(cash, 2)

    # 账户每日变化记录
    account_df = pd.DataFrame({
        '日期': [UserConfig.Start],
        '总资产': [UserConfig.CapitalBase],  # 总资产 = 持仓市值 + 可用现金
        '持仓市值': [0],    # 持仓市值 = 求和(个股持仓市值)
        '持仓盈亏': [0],    # 持仓盈亏 = 求和(个股持仓盈亏)
        '今日盈亏': [0],    # 今日盈亏 = 求和(个股今日盈亏)
        '可用现金': [UserConfig.CapitalBase],   # self.cash
    })

    def get_total_assets(self):
        last_line = self.account_df.tail(1)
        return last_line['总资产'].values[0]

    def append_account_df(self, trade_date, total_assets, total_cap,
                          profit_loss, profit_loss_today, cash):
        """
        每日账户记录 添加记录
        :param trade_date:
        :param total_assets:
        :param total_cap:
        :param profit_loss:
        :param profit_loss_today:
        :param cash:
        :return:
        """
        self.account_df.loc[len(self.account_df)] = [
            trade_date, total_assets, total_cap,
            profit_loss, profit_loss_today, cash
        ]


assets = Assets()
