# coding:utf-8

import pandas as pd


class Sort(object):

    def sort_pct_change(self, df):
        ascending = df.sort_values(by='pct_chg', ascending=True)
        ascending.index = range(len(ascending.index))
        descending = df.sort_values(by='pct_chg', ascending=False)
        descending.index = range(len(descending.index))
        return ascending[0:5], descending[0:5]


sort = Sort()
