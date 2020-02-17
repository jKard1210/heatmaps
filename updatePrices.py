import yfinance as yf
import csv
import numpy as np
import pandas
import statistics

import random
import pandas as pd

from flask import Flask
from sqlalchemy import create_engine
from sqlalchemy import Column, ForeignKey, Integer, String,Date,Float,ARRAY
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

from sqlalchemy.orm import sessionmaker

db = "postgres"
user = "dbmasteruser"
password = "rqM&y9]Hy>3o)eQ~WQA9:0O8vQilj;rO"
endpoint = 'ls-449373e22b69bfd9f636b38fc3ef306b0da46ee7.cw8iknkapczz.ca-central-1.rds.amazonaws.com'
engine = create_engine('postgresql://'+user+':'+password+'@'+endpoint+':5432/'+db)

Base.metadata.create_all(engine)
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

reader = csv.reader(open("russell.csv", "r", encoding="utf-8-sig"), delimiter=",")
companies = list(reader)
print(companies)
linesNew = []
i = 0
for ticker in companies:
    i = i+1
    print(i)
    print(ticker)
    lineCurr = []
    lineCurr.append(ticker[0])
    ticker = ticker[0]
    try:
        comp = yf.Ticker(ticker)
        category = str(comp.info["sector"])
        try:
            marketCap = float(comp.info["marketCap"]['raw'])
        except:
            marketCap = float(comp.info["marketCap"])


        try:
            midpoint = (float(comp.info["ask"]['raw']) + float(comp.info["bid"]["raw"])) / 2
        except:
            midpoint = (float(comp.info["ask"]) + float(comp.info["bid"])) / 2

        try:
            dayReturn = (midpoint/float(comp.info["previousClose"]["raw"]) - 1)*100
        except:
            dayReturn = (midpoint/float(comp.info["previousClose"]) - 1) * 100

        lineCurr.append(category)
        if(marketCap != None):
            lineCurr.append(marketCap)
        else:
            lineCurr.append(0)
        lineCurr.append(dayReturn)

    except Exception as e:
        lineCurr.append("N/A")
        lineCurr.append(1)
        lineCurr.append(1)
    linesNew.append(lineCurr)
df = pd.DataFrame(np.array(linesNew))
df.columns = ['Ticker','Category','Size','Return']
df = df.dropna()
df = df.astype({'Size': 'float'})
df = df.astype({'Return': 'float'})

df.to_sql('russellPrices', engine, if_exists='replace')