from part2.ml import CustomerComplaints

from settings import CUSTOMER_COMPLAINTS_HDFS

cc = CustomerComplaints()
sc = cc.sc

customer_complaints = sc.textFile(CUSTOMER_COMPLAINTS_HDFS)
cleaned_data = cc.data_cleansing(customer_complaints)

"""
print(cleaned_data.count())
print(cleaned_data.take(1))
"""
## Kwsta sunexise apo edw
"""
words = cleaned_data.flatMap(lambda x : x[2].split(' '))
lower_case_words = words.map(lambda x : x.lower())
distinct_words = lower_case_words.distinct()
only_words = distinct_words.filter(lambda x : bool(re.match("^[a-z]*$",x)))
final_words = only_words.filter(lambda x : x not in STOP_WORDS)
"""
final_words = cc.only_distinct_words(cleaned_data)

for i in final_words.take(100):
    print (i)
