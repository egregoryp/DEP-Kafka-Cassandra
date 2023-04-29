# Bootcamp Kafka y Spark Streaming

1. To start use the "instalaciones.docx" to configure spark, zookeeper and kafka in Windows
2. Use instructions in every lab to practice
3. for the project We need to download DataStax Cassandra and CDATA driver to connect ODBC with Cassandra
4. in Data Directory we have sample data for all labs
5. Project (files name starting with z#.Proy)

![alt text](https://github.com/egregoryp/DEP-Kafka-Cassandra-Project/blob/main/zStreamingProjectArquitecture.png?raw=true)

#### Project description
- **Reading**
    - Product searches in Ecommerce App (Kafka Producer in python - **Topico "product-search"**)
- **Data structures**
    - Json Streams of product searches with Customer ID and Location ID
    - Join with Location Catalog in csv file
    - Join with Customers Table in Cassandra Database
- **Aggregation**
    - Number of product searches by location and Customer
- **Output in file or other topic**
    - **Topic "product-customer-qty"**
- **Joins**
    - Product Search
    - Join with Location Catalog in csv file
    - Join with Customers Table in Cassandra Database

**Steps to implement:**
 1. Use files starting with z1.Proy
 2. Configure Cassandra Database using DataStax
 3. Create DB spark_db in Cassandra
 4. Create Tables Customer and Customer_search (file zCustomers.cql)
 5. Insert Customer Data
 6. Download CDATA Driver for ODBC to connect with PowerBI

 Note: extra file to generate files as an example is ProyCustomerFileWriter.ipynb
