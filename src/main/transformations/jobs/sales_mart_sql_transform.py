from pyspark.sql.functions import *
from pyspark.sql.window import Window
from resources.dev import config
from src.main.write.database_write import DatabaseWriter

#calculation for customer mart
#find out the customer total purchase every month
#write the data into MySQL table
def sales_mart_calculation_table_write(final_customer_data_mart_df):
    window = Window.partitionBy("sales_person_id", "sales_month", "store_id")
    rank_window = Window.partitionBy("store_id", "sales_month").orderBy(col("total_sales").desc())
    df = final_customer_data_mart_df.withColumn("sales_month", substring(col("sales_date"), 1, 7)) \
        .withColumn("total_sales", sum(col("total_cost")).over(window)) \
        .select("store_id", "sales_person_id",
                concat(col("sales_person_first_name"), lit(" "), col("sales_person_last_name")).alias("full_name"),
                "sales_month", "total_sales") \
        .distinct()

    rank_df = df.withColumn("ranking", rank().over(rank_window)) \
            .withColumn("incentive", when(col("ranking")==1, 0.01*col("total_sales")).otherwise(lit(0))) \
            .withColumn("incentive", round(col("incentive"),2)) \
            .select("store_id", "sales_person_id", "full_name", "sales_month",
                                                 "total_sales", "incentive")
    rank_df.show()
    #Write the Data into MySQL customers_data_mart table
    db_writer = DatabaseWriter(url=config.url,properties=config.properties)
    db_writer.write_dataframe(rank_df,"sales_team_data_mart")

