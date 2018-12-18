# -*- coding: utf-8 -*-
"""
Created on Mon Dec 17 21:11:47 2018

@author: Lenovo
"""
import pandas as pd

df=[]

sentiDic={}
resultfile=open('scratchBayesResult.txt','r')
line=resultfile.readline()
while line:
    line=line.rstrip()
    tmpSplit=line.split('\t')
    tmpSplit
    sentiDic[tmpSplit[0]]=tmpSplit[2]
    print(tmpSplit[1])
    line=resultfile.readline()
    
for i in range(996):
    tmpdic={}
    filename=str(i)+'.txt'
    f=open(filename,'r')
    line=f.read()
    tmpdic['atri']=sentiDic[str(i)]
    tmpdic['index']=str(i)
    tmpdic['context']=line
    print(str(i))
    df.append(tmpdic)
    
result=pd.DataFrame(df)
result.set_index(['index'], inplace = True) 
result.to_csv('scratchBayesResultCpr.csv')

    
    
