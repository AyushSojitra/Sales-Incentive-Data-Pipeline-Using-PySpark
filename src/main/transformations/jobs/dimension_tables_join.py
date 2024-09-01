from pyspark.sql.functions import *
from src.main.utility.logging_config import *
#enriching the data from different table
def dimesions_table_join(final_df_to_process,
                         customer_table_df,store_table_df,sales_team_table_df):

    logger.info("Joining the final_df_to_process with customer_table_df ")
    s3_customer_df_join = final_df_to_process.alias("s3_data") \
        .join(customer_table_df.alias("ct"),
              col("s3_data.customer_id") == col("ct.cust_id"),"inner") \
        .drop("product_name","price","quantity","additional_column",
              "cust_id")

    s3_customer_df_join.printSchema()

    logger.info("Joining the s3_customer_df_join with store_table_df ")
    s3_customer_store_df_join= s3_customer_df_join.join(store_table_df,
                             store_table_df["id"]==s3_customer_df_join["store_id"],
                             "inner")\
                        .drop("id","store_pincode","store_opening_date","reviews")

    s3_customer_store_df_join.printSchema()

    logger.info("Joining the s3_customer_store_df_join with sales_team_table_df ")
    s3_customer_store_sales_df_join = s3_customer_store_df_join.join(sales_team_table_df.alias("st"),
                         col("st.id") == s3_customer_store_df_join[
                             "sales_person_id"],
                         "inner") \
                        .drop("id")

    s3_customer_store_sales_df_join.printSchema()

    return s3_customer_store_sales_df_join

