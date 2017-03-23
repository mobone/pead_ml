from multiprocessing import Queue, Process
import requests
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
from random import randint
import numpy as np
import requests_cache
from time import sleep
import io
import sys
import time
import random

class announcement():
    def __init__(self, df, today):

        if len(sys.argv) == 0 or not today:
            #print('setting up cache')
            requests_cache.install_cache('estimize_cache')
        self.conn = sqlite3.connect('estimize_data.sqlite', timeout=30)

        self.df = df
        #print(self.df)

        self.get_yahoo()
        self.add_day_if_amc()
        self.get_change()
        self.get_day_change()
        #self.get_rev_surprise()
        self.get_eps_growth_rate()
        self.get_revs_growth_rate()
        self.get_avg_eps_beat_rate()
        self.get_average_ROI()
        self.get_industry()
        self.get_sue()

        self.df = pd.DataFrame(self.df).transpose()
        #print(self.df)

    def add_day_if_amc(self):
        if self.df['Time'] == "AMC":
            date = datetime.strptime(self.df['Date'], "%Y-%m-%d")
            date + timedelta(days=1)
            while date.isoweekday()>=6:
                date + timedelta(days=1)
            self.df['Date'] = str(date).split(" ")[0]


    def get_yahoo(self):
        try:
            url = "http://chart.finance.yahoo.com/table.csv?s={0}&a=12&b=01&c=2010&d=12&e=01&f=2018&g=d&ignore=.csv".format(self.df['Symbol'])
            data = io.StringIO(requests.get(url).text)
            self.yahoo_df = pd.read_csv(data)
        except Exception as e:
            self.yahoo_df = None

    def get_eps_growth_rate(self):
        # (q315-q314)/q315
        self.df['EPS_Growth'] = None
        try:
            prev_qtr = self.df['Qtr'][:2]+str(int(self.df['Qtr'][2:])-1)

            sql = "select `EPS` from estimize_processed where Symbol='%s' and Qtr = '%s'" % (self.df['Symbol'], prev_qtr)
            df = pd.read_sql(sql, self.conn)
            if not df.empty:
                eps_growth_rate = (float(self.df['EPS']) - float(df['EPS'])) / float(df['EPS'])

                self.df['EPS_Growth'] = eps_growth_rate

        except Exception as e:
            print(e)


    def get_revs_growth_rate(self):
        # (q315-q314)/q315
        self.df['Rev_Growth'] = None
        try:
            prev_qtr = self.df['Qtr'][:2]+str(int(self.df['Qtr'][2:])-1)

            sql = "select `Revs` from estimize_processed where Symbol='%s' and Qtr = '%s'" % (self.df['Symbol'], prev_qtr)
            df = pd.read_sql(sql, self.conn)
            if not df.empty:
                eps_growth_rate = (float(self.df['Revs']) - float(df['Revs'])) / float(df['Revs'])

                self.df['Rev_Growth'] = eps_growth_rate
        except Exception as e:
            print("here", e)



    def get_avg_eps_beat_rate(self):
        # average percent beat for last 4 quarters
        self.df['Avg_Beat'] = None
        self.df['Avg_Estimize_Beat'] = None
        try:
            sql = "select `Percent_Beat` from estimize_processed where Symbol='%s' order by Date desc limit 4" % (self.df['Symbol'])
            df = pd.read_sql(sql, self.conn)

            if len(df)>=3:
                avg_beat = sum(df['Percent_Beat'].astype(float))/len(df['Percent_Beat'])

                self.df['Avg_Beat'] = avg_beat

        except Exception as e:
            #print("here 2", e)
            pass

        try:
            sql = "select `Estimize_Percent_Beat` from estimize_processed where Symbol='%s' order by Date desc limit 4" % (self.df['Symbol'])
            df = pd.read_sql(sql, self.conn)

            if len(df)>=3:
                avg_beat = sum(df['Estimize_Percent_Beat'].astype(float))/len(df['Estimize_Percent_Beat'])

                self.df['Avg_Estimize_Beat'] = avg_beat

        except Exception as e:
            #print("here 3", e)
            pass


    def get_average_ROI(self):
        # average return of the last four quarters
        self.df['Avg_ROI'] = None
        try:
            sql = "select `ROI` from estimize_processed where Symbol='%s' order by Date desc limit 4" % (self.df['Symbol'])
            df = pd.read_sql(sql, self.conn)

            if len(df)>=3:

                avg_roi = sum(df['ROI'])/len(df['ROI'])

                self.df['Avg_ROI'] =avg_roi

        except Exception as e:
            #print("here 4", e)
            pass

    def get_industry(self):
        self.df['Industry'] = None
        try:
            html_text = requests.get("http://finviz.com/quote.ashx?t="+self.df['Symbol']).text
            df = pd.read_html(io.StringIO(html_text))[2]

            industry = df[5][6].split(" | ")[0]
            industry_num = None

            if industry=="Basic Materials":
                industry_num = 0
            elif industry=="Conglomerates":
                industry_num = 1
            elif industry=="Consumer Goods":
                industry_num = 2
            elif industry=="Financial":
                industry_num = 3
            elif industry=="Healthcare":
                industry_num = 4
            elif industry=="Industrial Goods":
                industry_num = 5
            elif industry=="Services":
                industry_num = 6
            elif industry=="Technology":
                industry_num = 7
            elif industry=="Utilities":
                industry_num = 8

            self.df['Industry'] = industry_num
        except Exception as e:
            #print("here 5", e)
            pass


    def get_sue(self):
        self.df['SUE'] = None
        self.df['Estimize_SUE'] = None
        try:
            sql = "select Surprise as SUE from estimize_processed where Symbol == '%s' order by Date desc limit 4;" % (self.df['Symbol'])

            df = pd.read_sql(sql, self.conn)

            if len(df)>=3 and sue != 0:
                sue = np.std(df['SUE'].astype('float'));
                sue = (float(self.df['Surprise'])) / sue

                self.df['SUE'] = sue

        except Exception as e:
            #print("here 6", e)
            pass


        try:
            sql = "select Estimize_Surprise as SUE from estimize_processed where Symbol == '%s' order by Date desc limit 4;" % (self.df['Symbol'])

            df = pd.read_sql(sql, self.conn)

            if len(df)>=3:
                sue = np.std(df['SUE'].astype('float'));
                sue = (float(self.df['Estimize_Surprise'])) / sue

                self.df['Estimize_SUE'] = sue

        except Exception as e:
            #print("here 7", e)
            pass


    def get_day_change(self):
        self.df['Before_ROI'] = None

        try:


            start_date = str(self.df['Date']).split(" ")[0]

            start_index = self.yahoo_df[self.yahoo_df['Date']==start_date].index

            open = self.yahoo_df.iloc[start_index]['Open'].values[0]
            close = self.yahoo_df.iloc[start_index+3]['Close'].values[0]

            overnight_roi = (open-close)/close

            self.df['Before_ROI'] = (overnight_roi * 100)
        except Exception as e:
            #print("here 8", e)
            pass


    def get_change(self):
        self.df['ROI'] = None
        self.df['Open'] = None
        self.df['Close'] = None
        try:
            start_date = str(self.df['Date']).split(" ")[0]

            change_df = self.yahoo_df[self.yahoo_df['Date']>=start_date].tail()
            change_df = change_df.reset_index(drop=True)


            open = change_df.ix[4,'Open']
            close = change_df.ix[0,'Close']

            roi = (close - open)/open
            if roi<2 and roi>-2:
                self.df['ROI'] = (roi * 100)
                self.df['Open'] = open
                self.df['Close'] = close
        except Exception as e:
            print("get change exception", e)
