from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lending_cart").getOrCreate()

customer_schema = 'member_id string, emp_title string, emp_length string,home_ownership string, annual_inc float, addr_state string, zip_code string,country string, grade string, sub_grade string, verification_status string,tot_hi_cred_lim float, application_type string, annual_inc_joint float,verification_status_joint string'

customers_raw_df = spark.read.format("csv").option("header",True).schema(customer_schema).load("C:\Users\sahas\OneDrive\Desktop\trendytech_notes\lending_cart_dataset\accepted_2007_to_2018Q4")

customers_raw_df.show()

# calling function 1