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


result = complaints11.map(lambda x : my_func(x, my_words, lexikon_size, synolo_keimenwn))

"""
[('', 184), ('still', 104213), ('full', 44733), ('score', 39032), ('account', 478034), ('two', 53154), ('also', 136231), ('even', 90215), ('phone', 107445), ('told', 211129)]
('Credit reporting credit repair services or other personal consumer reports', SparseVector(200, {1: 4.0, 3: 8.0, 6: 1.0, 8: 3.0, 9: 3.0, 11: 2.0, 12: 2.0, 14: 3.0, 21: 4.0, 28: 1.0, 31: 3.0, 37: 1.0, 42: 1.0, 43: 1.0, 44: 3.0, 54: 1.0, 78: 2.0, 90: 1.0, 102: 3.0, 113: 2.0, 134: 1.0, 174: 1.0}))
(label, (lexikon_size,{id_le3hs:plh8os emfanisewn sthn protash}))

tf = # emfanisewn sthn protash / #plh8os le3ewn (gia na ypologisw to # le3ewn a8roizw ta plh8h poy briskontai se {}

idf = prepei na ypologisw oi le3eis tou lexikon se posa keimena ypologizontai
opws alla3e o kapsalis to erwthma 3, ka8e keimeno exei mono tis diaforetikes le3eis
trexoyme word_count 3ana kai briskoyme ta plh8h poy mas endiaferoun


"""
for i in result.take(100):
    print(i)
