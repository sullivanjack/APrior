from pyspark import SparkContext
import itertools

# ----------------------- Main -----------------------

sc = SparkContext("local", "first app")
file1 = "browsing.txt"
file2 = "test.txt"
fileData = sc.textFile(file1).cache()
minSupport = 100

print(sc._conf.get('spark.executor.memory'))
print(sc._conf.get('spark.driver.memory'))

# ----------------------- Functions -----------------------

# This function will take all the possible combinations (of 2) in each transaction and add them to the map 
# Sorting each transaction alphabetically is critical to make sure all combinations are the same and you don't end up with (a,b) and (b,a)
def createMaps(transactions, size):
    doublesMap = []
    for transaction in transactionList:
        sortedTrans = sorted(transaction)
        allCombos = itertools.combinations(sortedTrans, size)
        for combo in allCombos:
            doublesMap.append( (combo, 1) )
    return doublesMap


# This function does a lot, idk a better way to do this though.
def doublesConfidence(combo, items):
    # ((a, b), conf), ((b, a), conf)
    first = combo[0][0]
    second = combo[0][1]
    firstConf = 0.1
    secondConf = 0.1
    conf = combo[1]

    for item in items:
        i = item[0]
        if(i == first):
            firstConf = conf/item[1]
        if(i == second):
            secondConf = conf/item[1]
    return [ ((first, second), firstConf), ((second, first), secondConf) ]

# This method really sucks
def triplesConfidence(combo, items):
    first = combo[0][0]
    second = combo[0][1]
    third = combo[0][2]
    firstConf = 0.1
    secondConf = 0.1
    thirdConf = 0.1
    conf = combo[1]

    for item in items:
        i = item[0]
        if(i == (first, second) ):
            firstConf = conf/item[1]
        if(i == (first, third) ):
            secondConf = conf/item[1]
        if(i == (second, third) ):
            thirdConf = conf/item[1]
    return [ (((first, second), third), firstConf), (((first, third), second), secondConf), (((second, third), first), thirdConf)  ]

"""
Doubles confidence
[I1]=>[I2^I3] //confidence = sup(I1^I2^I3)/sup(I1) 
[I2]=>[I1^I3] //confidence = sup(I1^I2^I3)/sup(I2)
[I3]=>[I1^I2] //confidence = sup(I1^I2^I3)/sup(I3)  

Triples confidence
[I1^I2]=>[I3] //confidence = sup(I1^I2^I3)/sup(I1^I2) 
[I1^I3]=>[I2] //confidence = sup(I1^I2^I3)/sup(I1^I3) 
[I2^I3]=>[I1] //confidence = sup(I1^I2^I3)/sup(I2^I3)
"""


# ----------------------- Data Manipulation -----------------------

# Get the data from the file and list each individual item in the RDD [item1, item2, item3, item1, item3, ...]
allItems = fileData.flatMap(lambda s: s.strip().split(" "))

# Create RDD with each item and its support count, then sort it (Highest to Lowest), then remove items with < minSupport 
itemSupport = allItems.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1], False).filter(lambda x: x[1] >= minSupport) 

# Print the ItemSupport count so we know how big our data set we are working with is
# (itemSupport.count() choose 3) = X the number of triples combos we will be working with.
print(itemSupport.count())

# Create a list of each individual transaction (Each line in the file)
transactionList = list(fileData.map(lambda x: x.strip().split(" ")).toLocalIterator())

# Create a list of all the possible doubles of the itemSupport items
doubles = itertools.combinations(list(itemSupport.keys().collect()), 2)

# See function definiton, then mapreduce, then filter, then sort
doublesRDD = sc.parallelize(createMaps(transactionList, 2)).reduceByKey(lambda x, y: x + y).filter(lambda x: x[1] >= minSupport).sortBy(lambda x: x[1], False).cache()

# Calculate confidence
# Put item support into list from so we can manipulate it better 
itemSupportList = itemSupport.collect()
# Calcualte our confidence and sort
doubleConfidences = doublesRDD.flatMap(lambda x: doublesConfidence(x,  itemSupportList)).sortBy(lambda x: x[1], False).cache()

print(doubleConfidences.take(15))


# Now lets do it but with 3 instead of 2
triples = itertools.combinations(list(itemSupport.keys().collect()), 3)

doublesSupportList = doublesRDD.collect()
triplesRDD = sc.parallelize(createMaps(doublesSupportList, 3)).reduceByKey(lambda x, y: x + y).filter(lambda x: x[1] >= minSupport).sortBy(lambda x: x[1], False).cache()

triplesConfidences = triplesRDD.flatMap(lambda x: triplesConfidence(x, doublesSupportList)).sortBy(lambda x: x[1], False).cache()

print(triplesConfidences.take(15))