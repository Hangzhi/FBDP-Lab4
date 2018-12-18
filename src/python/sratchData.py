# -*- coding: utf-8 -*-
"""
Created on Mon Dec 17 20:03:12 2018

@author: Lenovo
"""
import tushare as ts
import pandas as pd

pro = ts.pro_api('a597d0c4b910c849a5641e4f72f72f6a6bdb495238621f63cebf7da6')

df = pro.news(src='sina', start_date='20181121', end_date='20181217')

list_content=df['content']
count=0
for i in list_content:
    try:
        line=i.rstrip()
        line=line.rstrip('\n')
        title=str(count)
        if title in ['\n上期所49', '*ST皇455','\n【骅威478']:
            continue
        f=open(title+'.txt','w')
        f.write(line)
        count=count+1
        f.close()
    except ValueError:
        print(title)
        continue
    