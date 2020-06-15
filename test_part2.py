from part2.ml import CustomerComplaints

from settings import CUSTOMER_COMPLAINTS_HDFS

cc = CustomerComplaints()
sc = cc.sc

customer_complaints = sc.textFile(CUSTOMER_COMPLAINTS_HDFS)
cleaned_data = cc.data_cleansing(customer_complaints)

## Kwsta sunexise apo edw