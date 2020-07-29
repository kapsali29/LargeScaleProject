import re
from settings import CUSTOMER_COMPLAINTS_HDFS, STOP_WORDS
from helpers import init_spark
from math import log
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml.linalg import SparseVector
from pyspark.ml.feature import StringIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import MultilayerPerceptronClassifier
import time

def split_row(row):
    """This function received RDD from textFile and splits data"""
    data = row.split(",")
    if len(data) != 3:
        return None
    else:
        return data[0], data[1], data[2]


def data_cleansing(customer_complaints):
    """This function is used to remove dirty data from customer complaints"""
    # keep only rows starting with `201`
    filtered_complaints_rdd = customer_complaints.filter(lambda complaint: complaint.startswith('201'))
    # split rows and remove dirty ones
    splitted_rows = filtered_complaints_rdd.map(lambda x : split_row(x))
    cleaned_complaints = splitted_rows.filter(lambda complaint: complaint is not None)
    # keep only rows that have user comment
    keep_complaints = cleaned_complaints.filter(lambda complaint: complaint[2] != '')
    return keep_complaints


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
    # keep only the distinct words
    distinct_words = lower_case_words.map(lambda x: unique(x))
    # keep only the strings that include only letters
    only_words = distinct_words.map(lambda x: ( " ".join([word for word in x if bool(
            re.match("^[a-z]*$", word)) and word != '' and word not in STOP_WORDS])))       
    # keep only the words that are not stop words
    return only_words


def word_count_in_complaints(my_list, my_word):#my_search
    """
    This functions returns the number of complaints
    in which my_word is included
    """
    for i in my_list:
        if my_word == i[0]:
            return i[1]

def complaint_size(x,size):#my_sum
    """
    This functions calculates the amount of words
    in a complaint
    """
    summ=0
    for i in range(size):
        summ+=x[i]
    return summ



def tfidf_calc(my_list, my_words, lexikon_size, synolo_keimenwn):#my_func
    """
    This function receives (label,(N,(ind_i),(times_i))) and returns
    [N,(ind_i), (tfidf_i),label]
    """
    indexes = ()
    tfidf = ()
    a = [lexikon_size]
    le3eis_keimenou = complaint_size(my_list[1],lexikon_size)
    for i in range (lexikon_size):
        if my_list[1][i] != 0:
            indexes = indexes + (i,)
            pli8os_emfanisewn = word_count_in_complaints(my_words, lexikon[i])
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



spark, sc = init_spark()

customer_complaints = sc.textFile(CUSTOMER_COMPLAINTS_HDFS)
cleaned_data = data_cleansing(customer_complaints)


############# most common words
lexikon_size = 230
lexikon = cleaned_data.map(lambda x : (x[2])). \
        flatMap(lambda x : (x.split(" "))). \
        map(lambda x : x.lower()). \
        map(lambda x : (x,1)). \
        reduceByKey(lambda x, y: x + y). \
        map(lambda x : (x[1], x[0])). \
        sortByKey(ascending=False). \
        map(lambda x : x[1]). \
        filter(lambda x : bool(re.match("^[a-z]*$",x))). \
        filter(lambda x : x not in STOP_WORDS). \
        take(lexikon_size) 
############# TFIDF
time.sleep(20)

broad_com_words = sc.broadcast(lexikon)
complaints = cleaned_data.map(lambda x : (x[1],x[2].split(" "))). \
        map(lambda x : (x[0], [y for y in x[1] if y in broad_com_words.value])). \
        filter(lambda x : len(x[1]) != 0). \
        zipWithIndex(). \
        flatMap(lambda x : [((y, x[0][0], x[1]), 1) for y in x[0][1]]). \
        reduceByKey(lambda x, y : x + y). \
        map(lambda x : (x[0], (x[1], broad_com_words.value.index(x[0][0])))). \
        map(lambda x : ((x[0][2], x[0][1]), [(x[1][1], x[1][0])])). \
        reduceByKey(lambda x, y : x + y). \
        map(lambda x : (x[0][1], sorted(x[1], key = lambda y : y[0]))). \
        map(lambda x : (x[0], SparseVector(lexikon_size, [y[0] for y in x[1]], [y[1] for y in x[1]])))

time.sleep(20)

most_common_labels = complaints.map(lambda x : (x[0].lower(), 1)). \
        reduceByKey(lambda x, y: x + y). \
        map(lambda x : (x[1], x[0])). \
        sortByKey(ascending=False). \
        map(lambda x: x[1]). \
        take(4)
#we have 18 different labels, we choose the 4 most common

time.sleep(20)


final_complaints = complaints.filter(lambda x : x[0].lower() in most_common_labels)

synolo_keimenwn = final_complaints.count()
#337599 apo ta ~480000

data = only_distinct_words(cleaned_data). \
        flatMap(lambda x : x.split(" ")). \
        map(lambda x : x.lower()). \
        filter(lambda x : x in lexikon). \
        map(lambda x : (x,1)). \
        reduceByKey(lambda x, y: x + y)

#to data exei to pli8os diforetikwn keimenwn pou periexoyn tis le3eis toy le3ikou
my_words = data.collect()

time.sleep(20)

result = final_complaints.map(lambda x : tfidf_calc(x, my_words, lexikon_size, synolo_keimenwn)). \
        map(lambda x : [x[0],x[1],float_tuple(x[2]),x[3]]). \
        map(lambda x : (x[3],SparseVector(x[0],x[1],x[2])))
resultDF = result.toDF(["string_label","features"])

stringIndexer = StringIndexer(inputCol="string_label",outputCol="label")
stringIndexer.setHandleInvalid("skip")
stringIndexerModel = stringIndexer.fit(resultDF)
resultDF = stringIndexerModel.transform(resultDF)

train = resultDF.sampleBy('label', fractions = {0.0 :0.7, 1.0 :0.7, 2.0 :0.7, 3.0 :0.7}, seed = 10)

test = resultDF.subtract(train)

lr = LogisticRegression(maxIter=100, regParam=0.3, elasticNetParam=0.8, featuresCol='features', labelCol='label')
lrModel = lr.fit(train)

result = lrModel.transform(test)
predictionAndLabels = result.select("prediction", "label")
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
print("Test set accuracy = " + str(evaluator.evaluate(predictionAndLabels)))

#0.4114307678661187 lr
