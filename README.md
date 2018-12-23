本项目提交项目的结构如下所示

```java

├─data
│  │  features.txt	\\选择出的1500个特征词汇
│  │  KNNnegativeVecMatrix.txt \\ KNN训练使用的消极、中性、积极词汇
│  │  KNNneutralVecMatrix.txt
│  │  KNNpositiveVecMatrix.txt
│  │  NBayes.conf   \\NBayes训练使用的配置文件和train test向量文件
│  │  NBayes.test
│  │  NBayes.train
│  │  scratchTestData.txt \\爬取的新闻向量化文件
│  │  test.csv 	\\爬取的新闻集合
│  │  testDataScratch.zip  \\爬取新闻的原始文件
│  │  tfIdf_test.txt \\爬取新闻的tfidf
│  │  trainData.txt \\训练集向量文件
│  │  
│  ├─algorithm_test_vec  \\算法评估部分向量文件
│  │      KNNtest.txt
│  │      KNNtrain.txt
│  │      
│  └─tfIdf_train  \\训练集的tfidf
│          negativeTfIdf.txt
│          neutralTfIdf.txt
│          positiveTfIdf.txt
│          
├─results
│  │  scratchBayesResult.txt    \\Bayes的分类结果
│  │  scratchBayesResultCpr.csv \\Bayes分类结果与原文本对比的展示
│  │  ScratchKNNresult.txt	\\KNN分类结果
│  │  scratchKNNResultCpr.csv \\KNN的分类结果与原文本对比的展示
│  │  
│  └─algo_evaluation  \\算法评估中的部分结果
│          evalu1
│          evalu2
│          evalu3
│          evalu4
│          evalu5
|
│          
└─src
    ├─java
    │  │  Word2Vec.java  \\文本向量化
    │  │  
    │  ├─KNN   \\KNN训练算法
    │  │      Distance.java
    │  │      Instance.java
    │  │      KNNDriver.java
    │  │      ListWritable.java
    │  │      
    │  ├─NBayes  \\NBayes训练算法
    │  │      NaiveBayesConf.java
    │  │      NaiveBayesMain.java
    │  │      NaiveBayesTest.java
    │  │      NaiveBayesTrain.java
    │  │      NaiveBayesTrainData.java
    │  │      
    │  └─tfIdf	\\tfIdf计算
    │          TfIdf.java
    │          WordCountPerDoc.java
    │          WordsTotalDoc.java
    │          
    └─python
            featuresSelected.py  \\特征向量的筛选
            jsonFile.json  
            resultConvert.py  结果的格式转化
            sratchData.py \\新闻爬取
            trainIdConvert.py \\训练集的格式转化
  
```
在上述结构的基础上，data的weka文件夹内是使用weka的决策树算法进行预测的转换过训练集和训练结果。
