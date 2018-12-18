# -*- coding: utf-8 -*-
"""
Created on Sun Dec 16 20:15:56 2018

@author: Lenovo
"""

def convertIndex (sentiType):
    filename=sentiType+"VecMatrix.txt"
    f=open(filename,'r')
    newLines=[]
    line=f.readline()
    while line:
        splitLine=line.split('\t')
        tmpstr=sentiType+splitLine[0]+splitLine[1]
        newLines.append(tmpstr)
        line=f.readline()
    newFilename='new'+filename
    f.close()
    
    file_write=open(newFilename,'w')
    for var in newLines:
        file_write.writelines(var)
        #file_write.write('\n')
    file_write.close()

if __name__ == "__main__":
    sentiList=['negative','positive','neutral']
    for i in sentiList:
        convertIndex(i)