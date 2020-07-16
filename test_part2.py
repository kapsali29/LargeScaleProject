from part2.ml import CustomerComplaints
import re
from settings import CUSTOMER_COMPLAINTS_HDFS, STOP_WORDS

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml.linalg import SparseVector
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


cc = CustomerComplaints()
sc = cc.sc

customer_complaints = sc.textFile(CUSTOMER_COMPLAINTS_HDFS)
cleaned_data = cc.data_cleansing(customer_complaints)

"""
print(cleaned_data.count())
print(cleaned_data.take(1))
"""
## Kwsta sunexise apo edw

words = cleaned_data.flatMap(lambda x : x[2].split(' '))
lower_case_words = words.map(lambda x : x.lower())
distinct_words = lower_case_words.distinct()
only_words = distinct_words.filter(lambda x : bool(re.match("^[a-z]*$",x)))
final_words = only_words.filter(lambda x : x not in STOP_WORDS)
############# most common words
lexikon_size = 200
my_words = cleaned_data.map(lambda x : (x[2]))
my_words2 = my_words.flatMap(lambda x : (x.split(" ")))
my_words3 = my_words2.map(lambda x : (x,1))
my_words4 = my_words3.reduceByKey(lambda x, y: x + y)
my_words5 = my_words4.map(lambda x : (x[1], x[0]))
my_words6 = my_words5.sortByKey(ascending=False)
my_words7 = my_words6.map(lambda x : x[1].lower())
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
only_words = cc.only_distinct_words(cleaned_data)
"""
for i in complaints11.take(100):
    print(i)
"""
lexicon_size = 200

most_common_words = only_words.flatMap(lambda x: x[1].split(" ")). \
    map(lambda x: (x, 1)). \
    reduceByKey(lambda x, y: x + y). \
    sortBy(lambda x: x[1], ascending=False).map(lambda x: x[0]).take(lexicon_size)
"""
