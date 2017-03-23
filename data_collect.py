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
import sys
from collect_class import announcement
conn = sqlite3.connect('estimize_data.sqlite', timeout=30)

def get_data(date):
    symbols = []
    for page in range(1,5):
        if not today:
            requests_cache.install_cache('estimize_cache')

        url = "https://www.estimize.com/calendar/%s?_=0&direction=desc&page=%s&sort=popularity" % (date, page)
        data = requests.get(url).text
        yahoo_data = requests.get("https://biz.yahoo.com/research/earncal/%s.html" % (date.replace("/",""))).text

        replace = [' ‘', ' following-button-small', ' cal-col-instrument', ' release', ' popularity', ' datapoint', ' wall-street', ' estimize', ' actuals', ' estimate-status', ' reports']
        for r in replace:
            data = data.replace(r,"")
        replace = [("FQ", "Q"), ("<div class='tr'>" , "<tr>"), ("<div class='td'>", "<td>"), ("<br>", "<td>"), ("<div class='tbodys' data-tooltip-parent>", "<table>"), ("<div class='tbody' data-tooltip-parent>", "<table>") ]
        for i in replace:
            data = data.replace(i[0], i[1])
        for i in range(1,11):
            data = data.replace('<div class="popularity_bar pop_%s">' % (str(i)), "%s" % (str(i)))

        start = data.find("<table>")
        end = data.find("footer")

        data = data.encode(sys.stdout.encoding, errors="replace")
        data = data[start:end]
        df = None
        try:
            df = pd.read_html(data)[0]
            yahoo_data = pd.read_html(yahoo_data)[3][1].values
            df = df.drop_duplicates()
        except:
            break

        if len(df) == 20 and page != 1:
            break

        for i in df.iterrows():
            cur_symbol = i[1][0].split(" ")[0]
            symbols.append(cur_symbol)

            if cur_symbol in yahoo_data:
                df.loc[i[0],'Symbol'] = cur_symbol
            else:
                df.loc[i[0],'Symbol'] = None

        df = df.dropna(subset=['Symbol'])


        try:
            df = df[[1,2,3,6,7,8,9,10,11,'Symbol']]
            df['Date'] = date.replace("/", "-")
            df.columns = ['Qtr', 'Time', 'Popularity', 'EPS_Cons', 'Rev_Cons', 'EPS_Estimize', 'Revs_Estimize', 'EPS', 'Revs', 'Symbol', 'Date']

            if today:
                df = df[df['Time'] == today]
                print(df)

            df.to_sql("estimize_raw", conn, if_exists='append', index = False)
        except Exception as e:
            print(e)
            print(df)

        df = df.dropna(subset=['EPS_Cons', 'EPS'])

        df['Surprise'] = df['EPS']-df['EPS_Cons']
        df['Estimize_Surprise'] = df['EPS']-df['EPS_Estimize']
        df['Percent_Beat'] = (df['Surprise']/df['EPS_Cons'])*100
        df['Estimize_Percent_Beat'] = (df['Estimize_Surprise']/df['EPS_Estimize'])*100


        df['Revs_Percent_Beat'] = ((df['Revs']-df['Rev_Cons'])/df['Rev_Cons'])*100
        df['Estimize_Revs_Percent_Beat'] = ((df['Revs']-df['Revs_Estimize'])/df['Revs_Estimize'])*100

        df['Estimize_Percent_Diff'] = ((df['EPS_Cons'] - df['EPS_Estimize']) / df['EPS_Estimize'])*100
        df['Estimize_Rev_Percent_Diff'] = ((df['Rev_Cons'] - df['Revs_Estimize']) / df['Revs_Estimize'])*100
        for i in df.iterrows():
            q.put(i)



class eps_getter(Process):
    def __init__(self, q, today):
        Process.__init__(self)
        self.q = q
        self.today = today


    def run(self):
        self.conn = sqlite3.connect('estimize_data.sqlite', timeout=30)
        #print("Starting process")

        while True:
            data = self.q.get()

            x = announcement(data[1], self.today)

            x.df.to_sql("estimize_processed", self.conn, if_exists='append', index = False)


if __name__ == "__main__":
    q = Queue()
    if len(sys.argv)>1:                     # if being ran with arguments
        today = sys.argv[1]

        try:
            current_date = sys.argv[2]
        except:
            current_date = datetime.now()
        get_data(current_date)
    else:                                   # run all dates

        today = False
        try:
            conn = sqlite3.connect('estimize_data.sqlite', timeout=30)
            conn.execute("delete from estimize_processed")
            conn.execute("delete from estimize_raw")
            conn.commit()
        except Exception as e:
            print(e)

        current_date = datetime.strptime("01/01/2012 05:30:00", "%m/%d/%Y %H:%M:%S")
        end_date = datetime.now()

        # start the processes
        for i in range(4):
            eps_getter_object = eps_getter(q, today)
            eps_getter_object.start()

        while current_date<end_date:
            current_date = current_date + timedelta(days=1)
            if current_date.isoweekday()<6:
                get_data(datetime.strftime(current_date, "%Y/%m/%d"))
