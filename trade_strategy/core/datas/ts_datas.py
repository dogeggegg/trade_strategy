import datetime
from interval import Interval
import pandas as pd
import time
import warnings

from config.config import Config, UserConfig
from core.datas.ts_requests.ts_requests import ts_requests
from core.datas.ts_sql.ts_sql import ts_sql
from logger.logger import *

warnings.filterwarnings("ignore", "Mean of empty slice")


class TsInfo(object):

    ts_info_dict = dict()

    def save_stock_basic_df(self, df):
        self.ts_info_dict['stock_basic_df'] = df.drop(columns=['股票代码', '上市日期'])

    def get_stock_basic_df(self):
        stock_basic_df = self.ts_info_dict['stock_basic_df']
        return stock_basic_df


class GetData(TsInfo):

    def get_ts_daily_data(self, trade_date):
        """
        网络获取每日行情数据
        :param trade_date:
        :return:
        """
        try:
            # 获取 日线行情
            daily_df = ts_requests.get_daily(trade_date)
            # 获取 证券名称
            stock_basic_df = self.get_stock_basic_df()
            daily_df = pd.merge(daily_df, stock_basic_df, on='证券代码')
            # 获取 个股基本面指标
            daily_basic_df = ts_requests.get_daily_basic(trade_date)
            daily_basic_df.drop(columns=['日期', '收盘价'], inplace=True)
            df = pd.merge(daily_df, daily_basic_df, on='证券代码')
            return df
        except Exception as e:
            printe(str(e))
            return self.get_ts_daily_data(trade_date)

    @staticmethod
    def get_trade_cal(start_date, end_date=None):
        """
        获取各大交易所交易日历
        :param start_date: 开始日期 （格式：YYYY-MM-DD 下同），'2018-01-01'
        :param end_date: 结束日期， None表示今天
        :return: list ['2020-01-01', '2020-01-02']
        """
        df = ts_sql.get_trade_cal(start_date, end_date)
        df['日历日期'] = df['日历日期'].map(lambda x: x.strftime('%Y-%m-%d'))
        trade_cal_list = list(df['日历日期'])
        return trade_cal_list

    @staticmethod
    def get_ts_daily(start_date, end_date, ts_code=None,
                     filters=None, order=None, limit=None):
        """
        返回指定股票指定时间段日线
        :param ts_code:     "sz.000001"
        :param start_date:  "2020-08-04"
        :param end_date:    "2020-08-04"
        :param filters:    "滚动市盈率"
        :param order:    ("滚动市盈率", 'ASC')
        :param limit:    "0,10"
        :return:            pandas
            日期      股票代码  开盘价  最高价     最低价  收盘价  昨日收盘价     涨跌幅     成交量          成交额      换手率
           date       code     open     high      low    close preclose     pctChg    volume           amount      turn
0    2020-01-02  sz.002120  33.4900  33.5000  32.1500  32.9300  33.3000  -1.111100  12995282   427915261.8700  0.604500
        交易状态    滚动市盈率   市净率  滚动市销率    滚动市现率
        tradestatus      peTTM     pbMRQ     psTTM    pcfNcfTTM
                  1  27.432765  5.744737  2.540124  -150.111989
        """
        return ts_sql.get_ts_daily(start_date, end_date, ts_code, filters, order, limit)

    @staticmethod
    def get_industry_info(trade_date):
        industry_info_df = pd.DataFrame(columns=[
            '日期',
            '所属行业',
            '市盈率最小值',
            '市盈率最大值',
            '市盈率均值',
            '市盈率中位数',
            '市净率最小值',
            '市净率最大值',
            '市净率均值',
            '市净率中位数',
            '市销率最小值',
            '市销率最大值',
            '市销率均值',
            '市销率中位数',
            '股息率最小值',
            '股息率最大值',
            '股息率均值',
            '股息率中位数',
        ])
        df = ts_sql.get_ts_daily(trade_date, trade_date)
        industry_list = df['所属行业'].drop_duplicates()
        for industry in industry_list:
            industry_df = df[df['所属行业'] == industry]

            industry_pe = industry_df['滚动市盈率']
            pe_min = industry_pe.min()
            pe_max = industry_pe.max()
            pe_mean = industry_pe.mean()
            pe_median = industry_pe.median()

            industry_pb = industry_df['市净率']
            pb_min = industry_pb.min()
            pb_max = industry_pb.max()
            pb_mean = industry_pb.mean()
            pb_median = industry_pb.median()

            industry_ps = industry_df['滚动市销率']
            ps_min = industry_ps.min()
            ps_max = industry_ps.max()
            ps_mean = industry_ps.mean()
            ps_median = industry_ps.median()

            industry_dv = industry_df['滚动股息率']
            dv_min = industry_dv.min()
            dv_max = industry_dv.max()
            dv_mean = industry_dv.mean()
            dv_median = industry_dv.median()

            industry_info_df.loc[len(industry_info_df)] = [
                trade_date, industry,
                pe_min, pe_max, pe_mean, pe_median,
                pb_min, pb_max, pb_mean, pb_median,
                ps_min, ps_max, ps_mean, ps_median,
                dv_min, dv_max, dv_mean, dv_median,
            ]
        return industry_info_df


class TsDatas(GetData):

    @staticmethod
    def windup():
        ts_requests.windup()
        ts_sql.windup()

    @staticmethod
    def init_data_table_config():
        """
        初始化数据库和配置
        :return:
        """
        # 创建表
        ts_sql.create_ts_config_date_table()
        ts_sql.create_ts_daily_date_table()
        ts_sql.create_ts_trade_cal_date_table()
        ts_sql.create_ts_stock_basic_date_table()
        ts_sql.create_ts_dividend_date_table()
        ts_sql.create_ts_industry_date_table()
        # 同步数据库中的配置到类变量中
        ts_sql.init_update_daily_time()
        ts_sql.init_update_trade_cal_time()
        ts_sql.init_update_dividend_time()
        ts_sql.init_update_industry_time()

    def update_date(self, start_date, end_date):
        """
        更新指定日期的所有数据库
        :return:
        """
        # 更新个股基本信息数据库
        self.update_ts_stock_basic()
        # 更新交易日期数据库
        self.update_ts_trade_cal(end_date)
        # 更新日线个股行情数据库
        self.update_ts_daily(start_date, end_date)
        # 更新分红数据库
        self.update_ts_dividend(start_date, end_date)
        # 更新行业数据库
        self.update_ts_industry(start_date, end_date)

    @staticmethod
    def update_ts_trade_cal(end_date):
        """
        更新交易日期数据库
        :param end_date:
        :return:
        """
        update_trade_cal_time = ts_sql.get_update_trade_cal_time()
        if update_trade_cal_time:
            update_trade_cal_datetime = datetime.datetime.strptime(update_trade_cal_time, '%Y-%m-%d')
            end_datetime = datetime.datetime.strptime(end_date, '%Y-%m-%d')
            if update_trade_cal_datetime >= end_datetime:
                # 更新时间在需要数据的时间之后，无需更新
                return
        else:
            update_trade_cal_time = '1980-01-01'    # None 为最早的时间

        net_start = time.time()
        df = ts_requests.get_trade_cal(update_trade_cal_time, end_date)
        net_end = time.time()

        df = df[df['日历日期'] != update_trade_cal_time]    # 去掉更新日,否则该日会重复

        sql_start = time.time()
        ts_sql.add_trade_cal(df)
        sql_end = time.time()

        ts_sql.set_update_trade_cal_time(end_date)
        printf(f'更新交易日历数据库 trade_cal: {end_date} '
               f'网络耗时:{round(net_end - net_start, 3)}, '
               f'数据库耗时:{round(sql_end - sql_start, 3)}')

    def update_ts_stock_basic(self):
        net_start = time.time()
        df = ts_requests.get_stock_basic()
        net_end = time.time()
        sql_start = time.time()
        self.save_stock_basic_df(df)
        ts_sql.add_stock_basic(df)
        sql_end = time.time()
        printf(f'更新个股基本信息数据库 ts_stock_basic'
               f'网络耗时:{round(net_end - net_start, 3)}, '
               f'数据库耗时:{round(sql_end - sql_start, 3)}')

    def update_ts_daily(self, start_date, end_date):
        """
        更新日线个股行情数据库
        :param start_date:
        :param end_date:
        :return:
        """
        now_date_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        now_date, now_time = now_date_str.split(' ')
        trade_cal_list = self.get_trade_cal(start_date, end_date)
        update_daily_time = ts_sql.get_update_daily_time()
        if update_daily_time:
            index = trade_cal_list.index(update_daily_time)
        else:
            index = 0
        for trade_date in trade_cal_list[index:]:
            if trade_date == now_date:
                not_update_time = Interval.between('00:00:00', '16:00:00')
                if now_time in not_update_time:
                    printw(f'今日 {now_date} 数据还未更新，无法获取, 请于16点后获取')
                    return

            net_start = time.time()
            df = self.get_ts_daily_data(trade_date)
            net_end = time.time()

            sql_start = time.time()
            if trade_date == update_daily_time:
                # 删除 当日以及之后的记录，避免没有写入完全就停止的情况, 不可先删除，保证网络获取之后再删除
                ts_sql.delete_ge_trade_date_ts_daily(trade_date)
            ts_sql.add_ts_daily(df)
            sql_end = time.time()

            ts_sql.set_update_daily_time(trade_date)
            printf(f'更新每日行情数据库 ts_daily: {trade_date} '
                   f'网络耗时:{round(net_end - net_start, 3)}, '
                   f'数据库耗时:{round(sql_end - sql_start, 3)}')

    def update_ts_industry(self, start_date, end_date):
        now_date_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        now_date, now_time = now_date_str.split(' ')
        trade_cal_list = self.get_trade_cal(start_date, end_date)
        update_industry_time = ts_sql.get_update_industry_time()
        if update_industry_time:
            index = trade_cal_list.index(update_industry_time)
        else:
            index = 0
        for trade_date in trade_cal_list[index:]:
            if trade_date == update_industry_time:
                # 删除 当日以及之后的记录，避免没有写入完全就停止的情况
                ts_sql.delete_ge_trade_date_ts_industry(trade_date)
            if trade_date == now_date:
                not_update_time = Interval.between('00:00:00', '16:00:00')
                if now_time in not_update_time:
                    printw(f'今日 {now_date} 数据还未更新，无法获取, 请于16点后获取')
                    return
            net_start = time.time()
            net_end = time.time()
            sql_start = time.time()
            df = self.get_industry_info(trade_date)
            ts_sql.add_ts_industry(df)
            sql_end = time.time()
            ts_sql.set_update_industry_time(trade_date)
            printf(f'更新每日行业数据库 ts_industry: {trade_date} '
                   f'网络耗时:{round(net_end - net_start, 3)}, '
                   f'数据库耗时:{round(sql_end - sql_start, 3)}')

    @staticmethod
    def update_ts_dividend(start_date, end_date):
        """
        更新分红数据库
        :param start_date:
        :param end_date:
        :return:
        """
        start_year = int(start_date.split('-')[0])
        end_year = int(end_date.split('-')[0])
        year_list = list(range(start_year, end_year+1))
        update_dividend_year = ts_sql.get_update_dividend_time()
        if update_dividend_year:
            index = year_list.index(int(update_dividend_year))
        else:
            index = 0

        for year in year_list[index:]:
            start_date = f'{year}-01-01'
            end_date = f'{year}-12-31'
            ts_code_list = ts_sql.get_ts_daily_ts_code(start_date, end_date)
            if not ts_code_list:
                continue

            net_start = time.time()
            df = ts_requests.query_dividend_data(ts_code_list, year)
            net_end = time.time()

            sql_start = time.time()
            delete_date = f'{year}-01-01'
            ts_sql.delete_ge_trade_date_ts_dividend(delete_date)
            ts_sql.add_ts_dividend(df)
            sql_end = time.time()
            ts_sql.set_update_dividend_time(year)
            printf(f'更新个股分红数据库 ts_dividend: {year} '
                   f'网络耗时:{round(net_end - net_start, 3)}, '
                   f'数据库耗时:{round(sql_end - sql_start, 3)}')


ts_datas = TsDatas()


if __name__ == '__main__':
    logger.add_file_handler(Config.LogFilePath)

    # 初始化数据库、配置
    ts_datas.init_data_table_config()
    # 获取回测区间
    # __start = UserConfig.Start
    __end = UserConfig.End
    __start = '1980-01-01'
    __end = __end
    printf(f'更新数据库,区间: start:{__start}, end:{__end}')
    # 更新数据库
    ts_datas.update_date(__start, __end)
