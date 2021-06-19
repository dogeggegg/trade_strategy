# coding:utf-8


from core.datas.ts_sql.sql_oper import SQLOper


class TsConfig(SQLOper):

    def create_ts_config_date_table(self):
        # 表 ts_config
        create_ts_config_date_table_sql = (
            "CREATE TABLE IF NOT EXISTS ts_config("
            "variable_name VARCHAR(128),"
            "value VARCHAR(128),"
            "set_time timestamp,"
            "UNIQUE KEY variable_name_unique(variable_name)"
            ");"
        )
        self.execute_sql(create_ts_config_date_table_sql)

    def get_variable(self, variable_name):
        get_variable_sql = (
            f"SELECT value FROM ts_config WHERE variable_name='{variable_name}';"
        )
        result = self.execute_sql(get_variable_sql)
        if result:
            value = result[0][0]
            return value
        else:
            return None

    def set_variable(self, variable_name, value):
        if value is None:
            set_variable_sql = (
                f"INSERT INTO ts_config(variable_name, value, set_time) "
                f"VALUES('{variable_name}', NULL, now()) "
                f"ON DUPLICATE KEY UPDATE value=NULL, set_time=now();"
            )
        else:
            set_variable_sql = (
                f"INSERT INTO ts_config(variable_name, value, set_time) "
                f"VALUES('{variable_name}', '{value}', now()) "
                f"ON DUPLICATE KEY UPDATE value='{value}', set_time=now();"
            )
        self.execute_sql(set_variable_sql)

    UpdateDailyTime = None

    def init_update_daily_time(self):
        update_daily_time = self.get_variable('update_daily_time')
        self.UpdateDailyTime = update_daily_time

    def get_update_daily_time(self):
        return self.UpdateDailyTime

    def set_update_daily_time(self, value):
        self.set_variable('update_daily_time', value)
        self.UpdateDailyTime = value

    UpdateTradeCalTime = None

    def init_update_trade_cal_time(self):
        update_trade_cal_time = self.get_variable('update_trade_cal_time')
        self.UpdateTradeCalTime = update_trade_cal_time

    def get_update_trade_cal_time(self):
        return self.UpdateTradeCalTime

    def set_update_trade_cal_time(self, value):
        self.set_variable('update_trade_cal_time', value)
        self.UpdateTradeCalTime = value

    UpdateIndustryTime = None

    def init_update_industry_time(self):
        update_industry_time = self.get_variable('update_industry_time')
        self.UpdateIndustryTime = update_industry_time

    def get_update_industry_time(self):
        return self.UpdateIndustryTime

    def set_update_industry_time(self, value):
        self.set_variable('update_industry_time', value)
        self.UpdateIndustryTime = value

    UpdateDividendTime = None

    def init_update_dividend_time(self):
        update_dividend_time = self.get_variable('update_dividend_time')
        self.UpdateDividendTime = update_dividend_time

    def get_update_dividend_time(self):
        return self.UpdateDividendTime

    def set_update_dividend_time(self, value):
        self.set_variable('update_dividend_time', value)
        self.UpdateDividendTime = value

