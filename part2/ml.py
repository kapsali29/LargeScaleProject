from helpers import init_spark


class CustomerComplaints(object):
    def __init__(self):
        self.spark, self.sc = init_spark()

    @staticmethod
    def split_row(row):
        """This function received RDD from textFile and splits data"""
        data = row.split(",")
        if len(data) != 3:
            return None
        else:
            return data[0], data[1], data[2]

    def data_cleansing(self, customer_complaints):
        """This function is used to remove dirty data from customer complaints"""

        # keep only rows starting with `201`
        filtered_complaints_rdd = customer_complaints.filter(lambda complaint: complaint.startswith('201'))

        # split rows and remove dirty ones
        splitted_rows = filtered_complaints_rdd.map(self.split_row)
        cleaned_complaints = splitted_rows.filter(lambda complaint: complaint is not None)

        # keep only rows that have user comment
        keep_complaints = cleaned_complaints.filter(lambda complaint: complaint[2] != '')
        return keep_complaints
