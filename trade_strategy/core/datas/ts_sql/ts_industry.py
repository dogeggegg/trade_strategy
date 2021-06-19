
import pymysql
import pandas as pd


from core.datas.ts_sql.ts_config import TsConfig


class TsIndustry(TsConfig):

    def create_ts_industry_date_table(self):
        show_sql = (
            "SHOW TABLES LIKE 'ts_industry';"
        )
        result = self.execute_sql(show_sql)
        if not result:
            self.set_update_industry_time(None)
        create_ts_industry_date_table_sql = (
            "CREATE TABLE IF NOT EXISTS ts_industry("
            "ts_trade_date DATE,"
            "ts_industry VARCHAR(20),"
            "ts_pe_min float,"
            "ts_pe_max float,"
            "ts_pe_mean float,"
            "ts_pe_median float,"
            "ts_pb_min float,"
            "ts_pb_max float,"
            "ts_pb_mean float,"
            "ts_pb_median float,"
            "ts_ps_min float,"
            "ts_ps_max float,"
            "ts_ps_mean float,"
            "ts_ps_median float,"
            "ts_dv_min float,"
            "ts_dv_max float,"
            "ts_dv_mean float,"
            "ts_dv_median float,"
            "UNIQUE KEY ts_trade_date_industry(ts_trade_date, ts_industry)"
            ");"
        )
        self.execute_sql(create_ts_industry_date_table_sql)

    def delete_ge_trade_date_ts_industry(self, trade_date):
        """
        删除 大于等于这个日期的数据， 即删除这个时间点之后的数据
        :param trade_date:
        :return:
        """
        delete_sql = (
            f"DELETE FROM ts_industry WHERE ts_trade_date>='{trade_date}';"
        )
        self.execute_sql(delete_sql)

    def add_ts_industry(self, df):
        df.rename(columns={
            '日期': 'ts_trade_date',
            '所属行业': 'ts_industry',
            '市盈率最小值': 'ts_pe_min',
            '市盈率最大值': 'ts_pe_max',
            '市盈率均值': 'ts_pe_mean',
            '市盈率中位数': 'ts_pe_median',
            '市净率最小值': 'ts_pb_min',
            '市净率最大值': 'ts_pb_max',
            '市净率均值': 'ts_pb_mean',
            '市净率中位数': 'ts_pb_median',
            '市销率最小值': 'ts_ps_min',
            '市销率最大值': 'ts_ps_max',
            '市销率均值': 'ts_ps_mean',
            '市销率中位数': 'ts_ps_median',
            '股息率最小值': 'ts_dv_min',
            '股息率最大值': 'ts_dv_max',
            '股息率均值': 'ts_dv_mean',
            '股息率中位数': 'ts_dv_median',
        }, inplace=True)
        df.to_sql('ts_industry', self.engine, if_exists='append', index=False)

