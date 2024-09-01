import mysql.connector

def get_mysql_connection():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="spark_db"
    )
    return connection

# connection = get_mysql_connection()
# cursor = connection.cursor()
# query = """
#         INSERT INTO product_staging_table (id, file_name, file_location, created_date, updated_date, status)
#         VALUES (0, "bhavan", "aahaaa", "2024-02-02", "2024-03-03", "I")
#         """
# cursor.execute(query)
# connection.commit()
# cursor.execute("SELECT * FROM product_staging_table;")
# print(cursor.fetchall())
# cursor.close()
# connection.close()














# connection = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     password="password",
#     database="manish"
# )
#
# # Check if the connection is successful
# if connection.is_connected():
#     print("Connected to MySQL database")
#
# cursor = connection.cursor()
#
# # Execute a SQL query
# query = "SELECT * FROM manish.testing"
# cursor.execute(query)
#
# # Fetch and print the results
# for row in cursor.fetchall():
#     print(row)
#
# # Close the cursor
# cursor.close()
#
# connection.close()
