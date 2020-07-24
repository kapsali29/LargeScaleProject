from part2.ml import CustomerComplaints
import re
from settings import CUSTOMER_COMPLAINTS_HDFS, STOP_WORDS

from math import log
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml.linalg import SparseVector
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def unique(list1):

    # intilize a null list 
    unique_list = []

    # traverse for all elements 
    for x in list1:
        # check if exists in unique_list or not 
        if x not in unique_list:
            unique_list.append(x)
    return unique_list


def only_distinct_words(complaints):
        """This function returns the words, which are not stop words, from a list of complaints"""

        # split the words
        words = complaints.map(lambda x:  x[2].split(' '))

        # convert all words to lower case
        lower_case_words = words.map(lambda x: ([word.lower() for word in x]))

        # # keep only the distinct words
        distinct_words = lower_case_words.map(lambda x: unique(x))

        # keep only the strings that include only letters
        only_words = distinct_words.map(lambda x: ( " ".join([word for word in x if bool(
            re.match("^[a-z]*$", word)) and word != '' and word not in STOP_WORDS])))

        # keep only the words that are not stop words
        return only_words


cc = CustomerComplaints()
sc = cc.sc

customer_complaints = sc.textFile(CUSTOMER_COMPLAINTS_HDFS)
cleaned_data = cc.data_cleansing(customer_complaints)


############# most common words
lexikon_size = 200
my_words = cleaned_data.map(lambda x : (x[2]))
my_words2 = my_words.flatMap(lambda x : (x.split(" ")))
my_wordsk = my_words2.map(lambda x : x.lower())
my_words3 = my_wordsk.map(lambda x : (x,1))
my_words4 = my_words3.reduceByKey(lambda x, y: x + y)
my_words5 = my_words4.map(lambda x : (x[1], x[0]))
my_words6 = my_words5.sortByKey(ascending=False)
my_words7 = my_words6.map(lambda x : x[1])
my_words8 = my_words7.filter(lambda x : bool(re.match("^[a-z]*$",x)))
my_words9 = my_words8.filter(lambda x : x not in STOP_WORDS)
lexikon = my_words9.take(lexikon_size) 
############# TFIDF

broad_com_words = sc.broadcast(lexikon)
complaints = cleaned_data.map(lambda x : (x[1],x[2].split(" ")))
complaints2 = complaints.map(lambda x : (x[0], [y for y in x[1] if y in broad_com_words.value]))
complaints3 = complaints2.filter(lambda x : len(x[1]) != 0)
complaints4 = complaints3.zipWithIndex()
complaints5 = complaints4.flatMap(lambda x : [((y, x[0][0], x[1]), 1) for y in x[0][1]])
complaints6 = complaints5.reduceByKey(lambda x, y : x + y)
complaints7 = complaints6.map(lambda x : (x[0], (x[1], broad_com_words.value.index(x[0][0]))))
complaints8 = complaints7.map(lambda x : ((x[0][2], x[0][1]), [(x[1][1], x[1][0])]))
complaints9 = complaints8.reduceByKey(lambda x, y : x + y)
complaints10 = complaints9.map(lambda x : (x[0][1], sorted(x[1], key = lambda y : y[0])))
complaints11 = complaints10.map(lambda x : (x[0], SparseVector(lexikon_size, [y[0] for y in x[1]], [y[1] for y in x[1]])))
"""
('Credit reporting credit repair services or other personal consumer reports', SparseVector(200, {1: 4.0, 3: 8.0, 6: 1.0, 8: 3.0, 9: 3.0, 11: 2.0, 12: 2.0, 14: 3.0, 21: 4.0, 28: 1.0, 31: 3.0, 37: 1.0, 42: 1.0, 43: 1.0, 44: 3.0, 54: 1.0, 78: 2.0, 90: 1.0, 102: 3.0, 113: 2.0, 134: 1.0, 174: 1.0}))
"""


synolo_keimenwn = complaints11.count()

data = only_distinct_words(cleaned_data)
data1 = data.flatMap(lambda x : x.split(" "))
data2 = data1.map(lambda x : x.lower())
datak = data2.filter(lambda x : x in lexikon)
data3 = datak.map(lambda x : (x,1))
data4 = data3.reduceByKey(lambda x, y: x + y)

#to data4 exei to pli8os diforetikwn keimenwn pou periexoyn tis le3eis toy le3ikou
my_words = data4.collect()


def my_search(my_list, my_word):
    """
    This functions returns the number of complaints
    in which my_word is included
    """
    for i in my_list:
        if my_word == i[0]:
            return i[1]

def my_sum(x,size):
    """
    This functions calculates the amount of words
    in a complaint
    """
    summ=0
    for i in range(size):
        summ+=x[i]
    return summ



def my_func(my_list, my_words, lexikon_size, synolo_keimenwn):
    """
    This function receives (label,(N,(ind_i),(times_i))) and returns
    [N,(ind_i), (tfidf_i),label]
    """
    indexes = ()
    tfidf = ()
    a = [lexikon_size]
    le3eis_keimenou = my_sum(my_list[1],lexikon_size)
    for i in range (lexikon_size):
        if my_list[1][i] != 0:
            indexes = indexes + (i,)
            pli8os_emfanisewn = my_search(my_words, lexikon[i])
            temp = (my_list[1][i] / le3eis_keimenou) * log(synolo_keimenwn/pli8os_emfanisewn,10)
            tfidf = tfidf + (temp,)
    a.append(indexes)
    a.append(tfidf)
    a.append(my_list[0])
    return a

def float_tuple(tup):
    temp=()
    for t in tup:
        temp = temp + (float(t),)
    return temp



result = complaints11.map(lambda x : my_func(x, my_words, lexikon_size, synolo_keimenwn))
result1 = result.map(lambda x : [x[0],x[1],float_tuple(x[2]),x[3]])
result2 = result1.map(lambda x : (x[3],SparseVector(x[0],x[1],x[2])))
resultDF = result2.toDF(["string_label","features"])


"""
[Row(label='Mortgage', features=SparseVector(200, {7: 0.1424, 9: 0.0257, 14: 0.0259, 19: 0.0272, 22: 0.0659, 23: 0.0304, 28: 0.0389, 31: 0.0327, 33: 0.0318, 40: 0.0321, 56: 0.0384, 69: 0.0397, 72: 0.0462, 84: 0.0407, 106: 0.0433, 145: 0.0496, 154: 0.0485, 191: 0.1227, 192: 0.0488})), Row(label='Debt collection', features=SparseVector(200, {1: 0.462, 4: 0.0149, 9: 0.0347, 11: 0.0159, 14: 0.0175, 17: 0.0182, 20: 0.063, 23: 0.0617, 25: 0.0208, 26: 0.0205, 27: 0.0221, 32: 0.0867, 34: 0.0221, 41: 0.0249, 45: 0.0235, 60: 0.0267, 72: 0.0312, 76: 0.0265, 87: 0.028, 93: 0.0285, 150: 0.0381, 151: 0.0321, 160: 0.0317, 161: 0.0366, 166: 0.0331})), Row(label='Credit card or prepaid card', features=SparseVector(200, {16: 0.2762, 32: 0.2673, 142: 0.3951})), Row(label='Checking or savings account', features=SparseVector(200, {3: 0.0808, 10: 0.3048, 134: 0.2389, 157: 0.2354})), Row(label='Debt collection', features=SparseVector(200, {2: 0.0532, 6: 0.1194, 8: 0.1096, 18: 0.1526, 41: 0.1844}))]

"""

stringIndexer = StringIndexer(inputCol="string_label",outputCol="label")
stringIndexer.setHandleInvalid("skip")
stringIndexerModel = stringIndexer.fit(resultDF)
resultDF = stringIndexerModel.transform(resultDF)


(train, test) = resultDF.randomSplit([0.7, 0.3])
train = train.cache()
lr = LogisticRegression(maxIter=100, regParam=0.3, elasticNetParam=0.8, featuresCol='features', labelCol='label')
lrModel = lr.fit(train)

result = lrModel.transform(test)
predictionAndLabels = result.select("prediction", "label")
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
print("Test set accuracy = " + str(evaluator.evaluate(predictionAndLabels)))

