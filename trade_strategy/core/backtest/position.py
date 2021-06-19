import pandas as pd


class Position:

    # 当日持仓变量，每日初始化获取前一天的持仓
    cur_daily_position_df = pd.DataFrame(columns=[
        '日期',
        '证券代码',     # 外部传入
        '证券名称',     # 外部传入
        '持仓市值',     # 持仓市值 = 现价*持仓数量
        '持仓数量',     # 持仓数量 = 原持仓数量+持仓变化数量(正负, 策略传入)
        '可用数量',     # 可用数量 = 原持仓数量
        '现价',         # ts_datas.get_ts_daily(ts_code, date,date)['收盘价']
        '成本',         # 成本 = (原成本*原持仓数量+交易金额)/持仓数量  只有买卖的时候变化
        '持仓盈亏',     # 持仓盈亏 = 原持仓盈亏 + 今日盈亏
        # 今日盈亏 = (收盘价 - 前收盘价) * 一直持仓数量 + (收盘价 - 买入价) * 加仓数量 + (卖出价 - 收盘价) * 减仓数量
        '今日盈亏',
        '持仓占比',     # 持仓占比 = 持仓市值 / 总资产
    ])

    def init_cur_daily_position_df(self, trade_date):
        """
        初始化 当日持仓, 可用数量， 去除持仓为0的持仓
        :return:
        """
        self.cur_daily_position_df['可用数量'] = self.cur_daily_position_df['持仓数量']
        self.cur_daily_position_df['日期'] = trade_date
        index = self.cur_daily_position_df[self.cur_daily_position_df['持仓数量'] == 0].index
        if not index.empty:
            self.cur_daily_position_df.drop(index, inplace=True)
            self.cur_daily_position_df.reset_index(drop=True, inplace=True)

    def get_cur_daily_position_df(self):
        """
        获取当前持仓状态
        :return:
        """
        return self.cur_daily_position_df

    def get_cur_holding_stock_df(self, ts_code):
        """
        获取当前持仓的个股
        :param ts_code:
        :return:
        """
        cur_holding_stock_df = self.cur_daily_position_df[self.cur_daily_position_df['证券代码'] == ts_code]
        return cur_holding_stock_df

    def get_cur_holding_stock_col_sum(self, col):
        """
        获取当前持仓的 列 的和
        :return:
        """
        return self.cur_daily_position_df[col].sum()

    def add_cur_daily_position_df(self, trade_date, ts_code, ts_name, cap, hold_share, available_share,
                                  close, cost, profit_loss, profit_loss_today, proportion):
        self.cur_daily_position_df.loc[len(self.cur_daily_position_df)] = [
            trade_date, ts_code, ts_name, cap, hold_share, available_share,
            close, cost, profit_loss, profit_loss_today, proportion
        ]

    def set_cur_daily_position_df(self, ts_code, col, value):
        self.cur_daily_position_df.loc[self.cur_daily_position_df['证券代码'] == ts_code, col] = value

    # 持仓每日变化记录
    positions_df = pd.DataFrame(columns=[
        '日期',         # 外部传入
        '证券代码',     # 外部传入
        '证券名称',     # 外部传入
        '持仓市值',     # 持仓市值 = 现价*持仓数量
        '持仓数量',     # 持仓数量 = 原持仓数量+持仓变化数量(正负, 策略传入)
        '可用数量',     # 可用数量 = 原持仓数量
        '现价',         # ts_datas.get_ts_daily(ts_code, date,date)['收盘价']
        '成本',         # 成本 = (原成本*原持仓数量+交易金额)/持仓数量  只有买卖的时候变化
        '持仓盈亏',     # 持仓盈亏 = 原持仓盈亏 + 今日盈亏
        # 今日盈亏 = (收盘价 - 前收盘价) * 一直持仓数量 + (收盘价 - 买入价) * 加仓数量 + (卖出价 - 收盘价) * 减仓数量
        '今日盈亏',
        '持仓占比',     # 持仓占比 = 持仓市值 / 总资产
    ])

    def get_positions_df(self):
        """
        返回当前 持仓个股
        :return:
        """
        return self.positions_df

    def add_positions_df(self, cur_daily_position_df):
        """
        将当前持仓记录下来
        :param cur_daily_position_df:
        :return:
        """
        self.positions_df = pd.concat([self.positions_df, cur_daily_position_df], ignore_index=True)


position = Position()

