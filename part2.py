from helpers import init_spark

spark, sc = init_spark()


def split_row(row):
    """This function received RDD from textFile and splits data"""
    data = row.split(",")
    if len(data) != 3:
        return None
    else:
        return data[0], data[1], data[2]


customer_complaints = sc.textFile("file:///home/user/project-data/customer_complaints/customer_complaints.csv")
filtered_rdd = customer_complaints.filter(lambda complaint: complaint.startswith('201'))
splitted = filtered_rdd.map(split_row)
cleaned_complaints = splitted.filter(lambda complaint: complaint is not None)
keep_complaints = cleaned_complaints.filter(lambda complaint: complaint[2]!='')

print(keep_complaints.take(10))
