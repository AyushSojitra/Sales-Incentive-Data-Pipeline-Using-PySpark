import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))

from src.main.utility.s3_client_object import S3ClientProvider
from resources.dev import config
from src.main.utility.spark_session import *
from src.main.read.aws_read import S3Reader
from src.main.utility.encrypt_decrypt import *
from src.main.move.move_files import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime
from src.main.utility.my_sql_session import *
from dimension_tables_join import *
from src.main.write.parquet_writer import *
from customer_mart_sql_tranform_write import *
from sales_mart_sql_transform import *

S3ClientProvider = S3ClientProvider(decrypt(config.aws_access_key),decrypt(config.aws_secret_key))
s3_client = S3ClientProvider.get_client()
s3_reader = S3Reader()


sales_data_files = s3_reader.list_files(s3_client, config.bucket_name, config.s3_source_directory)
all_csv_files = [files for files in sales_data_files if files.endswith(".csv")]

valid_csv_files =[]
invalid_csv_files = []

spark = spark_session(decrypt(config.aws_access_key),decrypt(config.aws_secret_key))
for file in all_csv_files:
    file_columns = spark.read.csv(file, header=True, inferSchema=True).columns
    if (set(config.mandatory_columns) - set(file_columns)):
        invalid_csv_files.append(file)
    else:
        valid_csv_files.append(file)

print("Valid files: ", valid_csv_files)
print("Invalid files: ", invalid_csv_files)
#
# for files in invalid_csv_files:
#     move_s3_to_s3(s3_client, config.bucket_name, config.s3_source_directory, config.s3_error_directory, files.split('/')[-1])


# connection = get_mysql_connection()
# cursor = connection.cursor()
# id = 0
# if valid_csv_files:
#     for file in valid_csv_files:
#         id+=1
#         file_name = os.path.basename(file)
#         file_location = file[:-len(file_name)]
#         created_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#         updated_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#         status = "A"
#         query = f"INSERT INTO {config.product_staging_table} (id, file_name, file_location, created_date, updated_date, status)" \
#                 f"VALUES ({id}, '{file_name}', '{file_location}', '{created_date}', '{updated_date}', '{status}')"
#         logger.info(query)
#         cursor.execute(query)
#         connection.commit()
#         logger.info(f"Statement Inserted for {file_name}")
# cursor.close()
# connection.close()

# Creating a Schema for our new table with additional column
schema = StructType([
    StructField("customer_id",IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("total_cost",FloatType(), True),
    StructField("additional_column", StringType(), True)
])

final_df_to_process = spark.createDataFrame([],schema=schema)
# final_df_to_process.show()

for data in valid_csv_files:
    data_df = spark.read.csv(data, header=True, inferSchema=True)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info(f"Mandatory columns present at source is/are: {extra_columns}")
    if extra_columns:
        # data_df = data_df.withColumn("additional_column", concat_ws(", "), *extra_columns) \
        #     .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "quantity", "price",
        #             "total_cost", "additional_column")
        data_df = data_df.withColumn("additional_column", concat_ws(", ", *extra_columns)) \
            .select("customer_id","store_id","product_name","sales_date","sales_person_id","quantity","price","total_cost","additional_column")
    else:
        data_df = data_df.withColumn("additional_column",lit(None))\
        .select("customer_id","store_id","product_name","sales_date","sales_person_id","quantity","price","total_cost","additional_column")

    final_df_to_process = final_df_to_process.union(data_df)


logger.info("Final DataFrame ready for processing")
# final_df_to_process.show()


jdbc_url = "jdbc:mysql://localhost:3306/spark_db"
db_properties = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}

customer_table_df = spark.read.jdbc(url=jdbc_url, table="customer", properties=db_properties).select(
    col("customer_id").alias("cust_id"),"first_name", "last_name",
    col("address").alias("cust_address"), "pincode", "phone_number"
)
store_table_df = spark.read.jdbc(url=jdbc_url, table="store", properties=db_properties).select(
    "id", col("address").alias("store_address"), "store_manager_name"
)
sales_team_table_df = spark.read.jdbc(url=jdbc_url, table="sales_team", properties=db_properties).select(
    "id",
    col("first_name").alias("sales_person_first_name"),
    col("last_name").alias("sales_person_last_name"),
    col("address").alias("sales_person_address"),
    col("pincode").alias("sales_person_pincode"),
    "manager_id",
    "is_manager",
    "joining_date"
)

s3_customer_store_sales_df_join = dimesions_table_join(final_df_to_process,customer_table_df,store_table_df,sales_team_table_df)
# s3_customer_store_sales_df_join.show()


logger.info("Customer Data into Data Mart")

customer_data_mart = s3_customer_store_sales_df_join\
                    .select("customer_id",
                            "first_name", "last_name",
                            "cust_address", "pincode","phone_number",
                            "sales_date","total_cost")

customer_data_mart.show()
#
# parquet_writer = ParquetWriter("overwrite", "parquet")
#
# parquet_writer.dataframe_writer(customer_data_mart, config.customer_data_mart_table)

logger.info("sales data into data mart")
sales_team_data_mart = s3_customer_store_sales_df_join\
                    .select("store_id", "sales_person_id",
                            "sales_person_first_name", "sales_person_last_name",
                            "store_manager_name", "manager_id", "is_manager",
                            "sales_person_address", "sales_person_pincode",
                            "sales_date", "total_cost",
                            expr("SUBSTRING(sales_date, 1, 7) as sales_month"))

sales_team_data_mart.show()

# parquet_writer = ParquetWriter("overwrite", "parquet")
#
# parquet_writer.dataframe_writer(sales_team_data_mart, config.sales_team_data_mart_table)

# sales_team_data_mart.write.format("parquet") \
#             .option("header", "true") \
#             .mode("overwrite") \
#             .partitionBy("sales_month","store_id") \
#             .save(f"{config.sales_team_data_mart_table}/partitionedData")

print("sales team data mart done")

logger.info("Calculating customer every month purchase amount...")
customer_mart_calculation_table_write(customer_data_mart)

logger.info("Calculating sales person incentive amount...")
sales_mart_calculation_table_write(sales_team_data_mart)