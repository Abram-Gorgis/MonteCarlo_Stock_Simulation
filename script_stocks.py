# -*- coding: utf-8 -*-

import pandas as pd
from alpha_vantage.timeseries import TimeSeries
import time
api_key = '8AY2N70JLCKLZLOD'
ts = TimeSeries(key=api_key,output_format = 'pandas')

f=open("stock.txt","r")
f2 = open("stockdata.txt", "w")
f1= f.readlines() 
data2 =  pd.DataFrame()
for x in f1:
    data, meta_data = ts.get_daily(symbol=x.strip(),outputsize='full')
    name = x.strip()
    data2[name]=(data['4. close']/data['1. open'])-1

data2=data2.T
f2.write(data2.to_string())
print(data2)
