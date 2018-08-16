#import itertools
#from itertools import combinations
import sys
import pyspark
import collections
from collections import Counter
import time
import math

def phase1SON(s,totalNumberOfBaskets, listSingleton):
    def _phase1(iterable):
       
        LS = list(iterable)
        result = list()
        
        numberOfBasketInPartition = len(LS)
        tmp = float(numberOfBasketInPartition)/totalNumberOfBaskets
        
        threshold = math.ceil(tmp*s)
        #print "*********************"
        #print threshold
        #print "**********************"
        
        for i in listSingleton:
            
            tupleI = tuple(i)
            result.append(tupleI)
        #print "Calling constructCandidateItems2 k=2"
        #construct(LS,listSingleton,2, threshold, result)
        #constructCandidateItems2(LS,listSingleton,2, threshold, result)
        #k = 2
        #freqItems = constructCandidateItems2(LS,listSingleton,k, threshold, result)
        #print "return from constructCandidateItems2"
        ##if len(freqItems) != 0:
           # k += 1
            #freqItems = constructCandidateItems2(LS,listSingleton,k, threshold, result)
        
        #print "result"
        #print result
        
        k = 2
        freqItems = constructCandidateItems2(LS,listSingleton,k, threshold, result)
        
        while len(freqItems) != 0:
            k += 1
            freqItems =  constructCandidateItems2(LS,freqItems,k, threshold, result)
        
        return result
    return _phase1
	
def constructCandidateItems1(listFrequentItems, k):
    #print "Check singles"
    #print type(listFrequentItems)
    #print listFrequentItems
    
    '''
    ds = list()
    for i in listFrequentItems:
        for j in listFrequentItems:
            setUnion = i.union(j)
            if (len(setUnion) == k) and (setUnion not in ds):
                ds.append(setUnion)

    return ds
    '''
    #noOfChecks = 0
    ds = list()
    for x in listFrequentItems:
        index = listFrequentItems.index(x)
        for y in (listFrequentItems[(index+1):]):
            subA = x[:(k-2)]
            subB = y[:(k-2)]
            if set(subA) == set(subB):
                #noOfChecks += 1
                #subC = x[(k-2):]
                #subD = y[(k-2):]
                #union1 = (set(subC)).union(set(subD))
                #union2 = (set(x)).union(set(union1))
                #print union2
                union2 = (set(x)).union(set(y))
                ds.append(sorted(list(union2)))

           
    '''
    print "k = "
    print k
    print "ds"
    print ds
    '''
    #print "noOfChecks"
    #print noOfChecks
    return ds
	
def constructCandidateItems2(LS,listFrequentItems, k, s, result):
    if len(listFrequentItems) == 0:
        return
    else:
        #print "Calling constructCandidateItems1 for k = "+str(k)
        listCandidateItems1 = constructCandidateItems1(listFrequentItems,k)
        #print "Returned from constructCandidateItems1 for k = "+str(k)
        dicIntermediate = dict()
        newFreqItems = list()
        for i in LS:
            basketCombo = list()
            setBasket = set(i)
            for j in listCandidateItems1:
                sub = set(j)
                if sub.issubset(setBasket):
                    basketCombo.append(j)
            if len(basketCombo) != 0:
                for i in basketCombo:
                    t = tuple(sorted(i))
                    if t in dicIntermediate.keys():
                        dicIntermediate[t] += 1
                    else:
                        dicIntermediate[t] = 1
                        
        filter(dicIntermediate,newFreqItems,s,result)
        #print "length of freq item sets of size "+str(k)+" = "+str(len(newFreqItems))
        
        newFreqItems1 = sorted(newFreqItems)
        #print "newFreqItems"
        #print newFreqItems
        
        
        return newFreqItems1
        #constructCandidateItems2(LS,newFreqItems1,k+1,s,result)
		
def filter(dicIntermediate,newFreqItems,s,result):
    #print "Filter"
    for i in dicIntermediate.keys():
        if(dicIntermediate[i] >= s):
            listTemp = list(i)
            newFreqItems.append(listTemp)
            result.append(i)
			
def phase2SON(pass1FreqItemsList):
    def _phase2(iterable):
        #ls1 = list(pass1FreqItemsList)
        dicIntermediateSON = dict()
        LS = list(iterable)
        #LS = iterable
        #print "Inside phase2SON"
        for basket in LS:
            setBasket = set(basket)
            #print "setBasket"
            #print setBasket
            for candidate in pass1FreqItemsList:
            #for candidate in ls1:
                #print type(candidate)
                setCandidate = set([])
                if isinstance(candidate, int):
                    setCandidate.add(candidate)
                else:
                    setCandidate = set(candidate)
                setTemp = setCandidate.intersection(setBasket)
                
                if len(setTemp) == len(setCandidate):
                    if candidate in dicIntermediateSON.keys():
                        dicIntermediateSON[candidate] += 1
                        
                    else:
                        dicIntermediateSON[candidate] = 1
                        
        
        #print "dicIntermediateSON"
        #print dicIntermediateSON.iteritems()
        #print "returning dictionary from phase2SON"
        
        return dicIntermediateSON.iteritems()
    return _phase2
	

	
def writeToFile(ls2,fileName):
    #tf = "E:\\USC\\DataMining\\Assignment\\Assignment2\\Assignment_02\\temp\\output8.txt"
    thefile = open(fileName,'w')
    finalString = ""
    k = 1
    for item in ls2:
        word = ""
        tupleWord = ""
        if len(item) != k:
            k += 1
            finalString = finalString[:-1]
            finalString += "\n\n"
        for tupleVal in item:
            single = str(tupleVal) 
            word += single+","
        tupleWord += "("+word[:-1]+")"
        finalString += tupleWord+","
    finalString = finalString[:-1]
    print finalString
    thefile.write("%s\n" % finalString)
    thefile.close()
	

	
conf1 = (pyspark.SparkConf().setMaster("local[8]"))

start = time.time()


caseNo = sys.argv[1]
inputFile = sys.argv[2]
support = sys.argv[3]
'''
caseNo = "1"
#inputFile = "E:\\USC\\DataMining\\Assignment\\Assignment2\\Assignment_02\\Description\\Data\\Small2.csv"
#inputFile = "C:\\Users\\ankit\\Downloads\\ml-20m\\ratings.csv"
inputFile = "C:\\Users\\ankit\\Downloads\\ml-latest-small\\ratings.csv"
#inputFile = "C:\\Users\\ankit\\Downloads\\ml-20m\\ratings.csv"
support = "120"

'''


s = int(support)


if s > 500:
    sc = pyspark.SparkContext(conf = conf1)
else:
    sc = pyspark.SparkContext()


ratings = sc.textFile(inputFile)

#print "ratings.getNumPartitions()"
#print ratings.getNumPartitions()

#print "Creating RDD"
rdd = ratings.zipWithIndex().filter(lambda (row,index): index > 0).keys()
rdd2 = rdd.map(lambda x:x.split(','))
if caseNo == '1':
    rdd3_mapped = rdd2.map(lambda y : (int(y[0]),int(y[1])))
elif caseNo == '2':
    rdd3_mapped = rdd2.map(lambda y : (int(y[1]),int(y[0])))
rdd6 = rdd3_mapped.distinct().map(lambda nameTuple: (nameTuple[0], [ nameTuple[1] ])).reduceByKey(lambda a, b: a + b)
rdd7 = rdd6.values()
#print "created RDD"
#rdd8 = rdd7.collect()
#totalNumberOfBaskets = len(rdd8)
totalNumberOfBaskets = rdd7.count()
#print "totalNumberOfBaskets = "+str(totalNumberOfBaskets)


listSingles = list()
fm = rdd7.flatMap(lambda x :x).collect()
cntSingles = Counter(fm)
for item in cntSingles:
    if cntSingles[item] >= s:
        listTemp = list([item])
        listSingles.append(listTemp)

        
#print "listSingletons"
#print listSingles
     
#print "Calling phase1SON"
#pass1FreqItems = rdd7.mapPartitions(pass1APriory(s,totalNumberOfBaskets))
pass1FreqItems = rdd7.mapPartitions(phase1SON(s,totalNumberOfBaskets,listSingles))
#print "Return from pass1Apriory"
#pass1FreqItemsRDD = pass1FreqItems.map(lambda x:(x,1)).reduceByKey(lambda x,val:val)
#pass1FreqItemsList = pass1FreqItems.map(lambda x:(x,1)).reduceByKey(lambda x,val:val).keys().collect()
#print "print pass1FreqItemsList"
pass1FreqItemsList = pass1FreqItems.map(lambda x:(x,1)).reduceByKey(lambda x,val:val).keys().collect()
#print "pass1FreqItemsList"
#print pass1FreqItemsList

#local_freq_itemsets = sc.broadcast(pass1FreqItemsList)
#print "Calling phase2SON"
sonPhase2ItemCount = rdd7.mapPartitions(phase2SON(pass1FreqItemsList))
#sonPhase2ItemCount = rdd7.mapPartitions(phase2SON(pass1FreqItemsList))
#print "Returned from phase2SON"

#rddTest3 = sc.parallelize(sonPhase2ItemCount).map(lambda x:(x[0],x[1])).reduceByKey(lambda x,y : x+y).filter(lambda x : x[1] >= 4).keys().collect()
rddTest3 = sonPhase2ItemCount.map(lambda x:(x[0],x[1])).reduceByKey(lambda x,y : x+y).filter(lambda x : x[1] >= s).keys().collect()
ls1 = sorted(rddTest3)
#print "rddTest3"
ls2 = sorted(ls1, key=len)
#print ls2
end = time.time()
#print(end - start)
#fn1 = inputFile[-11:]
#fn2 = inputFile[:-4]
if( s > 1000):
    fn2 = "MovieLens.Big"
else:
    fn2 = "MovieLens.Small"
fileName = "Ankitha_Radhakrishna_SON_"+fn2+".case"+caseNo+"-"+support+".txt"
#print "fileName = "+fileName
writeToFile(ls2,fileName)
