# -*- coding: utf-8 -*-
"""
Created on Wed Dec 12 16:35:08 2018

@author: Hangzhi
"""
import json

negFeaturesDict={}
neuFeaturesDict={}
posiFeaturesDict={}

def loadDict(path,featuresDict):
    f=open(path,'r',encoding='utf-8')
    line=f.readline()
    while line:
        #temp=line.split('\t')
        #print(temp,line)
        word=line.split(',')[0];
        tfIdf=float(line.strip('\n').split('\t')[1]);
        if not featuresDict.__contains__(word):
            featuresDict[word]=tfIdf
            #print(word)
        else:
            preTfIdf=featuresDict[word]
            if preTfIdf<tfIdf:
                featuresDict[word]=tfIdf
        line=f.readline()
            
def getTopWords(d,n):
    result={}
    s = [(k, d[k]) for k in sorted(d, key=d.get, reverse=True)]
    #print(s)
    count=0
    for i in s:
        result[i[0]]=i[1]
        count+=1
        if count ==n:
            break
    #print(result)
    return result

#merge two dictionary and maintain the bigger value
def mergeDict(dica,dicb,dicc):
    result=dica
    for k in dicb:
        if not result.__contains__(k):
            result[k]=dicb[k]
        else:
            preValue=dica[k]
            if preValue<dica[k]:
                result[k]=dicb[k]
    for k in dicc:
        if not result.__contains__(k):
            result[k]=dicc[k]
        else:
            preValue=result[k]
            if preValue<result[k]:
                result[k]=dicc[k]
    return result
        
if __name__ == '__main__':
    negPath="data/negativeTfIdf/part-r-00000"
    neuPath="data/neutralTfIdf/part-r-00000"
    posiPath="data/positiveTfIdf/part-r-00000"
    
    loadDict(negPath,negFeaturesDict)
    loadDict(posiPath, posiFeaturesDict)
    loadDict(neuPath, neuFeaturesDict)

    negTOP500=getTopWords(negFeaturesDict,1000)
    posiTOP500=getTopWords(posiFeaturesDict,1000)
    neuTOP500=getTopWords(posiFeaturesDict,1000)
    print(negTOP500)
    tempJson=json.dumps(negTOP500,ensure_ascii=False)
    fileObject = open('jsonFile.json', 'w')
    fileObject.write(tempJson)
    fileObject.close()
    TOPwords=mergeDict(negTOP500,posiTOP500,neuTOP500)
    #print(TOPwords,len(TOPwords))
    TOP1500words=getTopWords(TOPwords,1500)
    #print(TOP1000words,len(TOP1000words))
    f=open('features.txt','w',encoding='utf-8')
    count=0
    for i in TOP1500words:
        count=count+1
        f.writelines(i)
        if count==1500:
            break
        f.write('\n')
    f.close()
    #print(len(negFeaturesDict))
    
    
    
    