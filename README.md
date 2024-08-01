# Olympic-Data-Azure-Data-Engineering

### Integrating Azure Data Factory and Databricks with Azure Synapse for Data Processing and Analysis

#### Introduction

In the modern data landscape, businesses require robust, scalable, and efficient tools to process and analyze vast amounts of data. Azure Data Factory, Azure Databricks, and Azure Synapse Analytics are three such powerful services offered by Microsoft Azure. These services provide comprehensive solutions for data integration, big data processing, and advanced analytics. In this article, we will delve into how these services can be integrated to create a seamless data pipeline, exemplified by a PySpark-based implementation for the Tokyo Olympic data analysis.

#### Azure Data Factory

Azure Data Factory (ADF) is a cloud-based data integration service that allows data professionals to create, schedule, and orchestrate data workflows at scale. It supports various data sources and destinations, making it ideal for extracting, transforming, and loading (ETL) data across on-premises and cloud environments.

**Key Features of Azure Data Factory:**

1. **Data Ingestion:** ADF can ingest data from various sources, including Azure Blob Storage, Azure SQL Database, and on-premises databases.
2. **Data Transformation:** Using data flows or Azure Databricks notebooks, ADF can transform raw data into meaningful insights.
3. **Scheduling and Monitoring:** ADF provides scheduling capabilities and monitoring tools to ensure the data workflows run as expected.

#### Azure Databricks

Azure Databricks is an Apache Spark-based analytics platform optimized for Azure. It provides a collaborative environment for data engineers, data scientists, and business analysts to work together on big data analytics projects.

**Key Features of Azure Databricks:**

1. **Apache Spark Integration:** Databricks offers a fully managed Apache Spark environment, enabling fast and scalable big data processing.
2. **Collaborative Notebooks:** It provides interactive notebooks for code development and collaboration, supporting languages like Python, Scala, and SQL.
3. **Integration with Azure Services:** Azure Databricks integrates seamlessly with other Azure services like Azure Data Lake Storage, Azure Synapse, and Azure Machine Learning.

#### Azure Synapse Analytics

Azure Synapse Analytics is an integrated analytics service that accelerates time to insight across data warehouses and big data systems. It combines big data and data warehousing into a single, integrated platform.

**Key Features of Azure Synapse Analytics:**

1. **Integrated Analytics:** Synapse combines big data and data warehousing, providing a unified experience for data ingestion, exploration, and analysis.
2. **Synapse SQL:** A scalable SQL engine for querying and managing relational data.
3. **Spark Pools:** Integrated Apache Spark pools for big data processing, data exploration, and machine learning.

#### Implementing a Data Pipeline for Tokyo Olympic Data

Let's explore a practical example where Azure Data Factory, Azure Databricks, and Azure Synapse Analytics are used to process and analyze Tokyo Olympic data. The data is stored in Azure Data Lake Storage and includes information on athletes, coaches, gender entries, medals, and teams.

**1. Setting Up Azure Data Factory for Data Ingestion:**

We begin by using Azure Data Factory to orchestrate the data ingestion from Azure Blob Storage (or Data Lake Storage) into our processing environment. This involves setting up linked services and datasets for the source and sink data stores.

**2. Data Processing with Azure Databricks:**

In Azure Databricks, we create a notebook to process and analyze the data. Here’s a breakdown of the main steps in the code:

- **Mounting Azure Data Lake Storage:**
  ```python
  configs = {
      "fs.azure.account.auth.type": "OAuth",
      "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
      "fs.azure.account.oauth2.client.id": "<client_id>",
      "fs.azure.account.oauth2.client.secret": '<client_secret>',
      "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<tenant_id>/oauth2/token"
  }

  dbutils.fs.mount(
      source="abfss://tokyo-olympic-data@tokyoolympicdata.dfs.core.windows.net",
      mount_point="/mnt/tokyoolympic",
      extra_configs=configs
  )
  ```

  This code mounts the Azure Data Lake Storage to the Databricks file system, allowing access to the data files.

- **Loading Data:**
  ```python
  athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolympic/raw-data/athletes.csv")
  coaches = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolympic/raw-data/coaches.csv")
  entriesgender = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolympic/raw-data/entriesgender.csv")
  medals = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolympic/raw-data/medals.csv")
  teams = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolympic/raw-data/teams.csv")
  ```

  Data is loaded into Spark DataFrames from the CSV files stored in the mounted directory.

- **Data Transformation and Analysis:**
  ```python
  entriesgender = entriesgender.withColumn("Female", col("Female").cast(IntegerType())) \
                               .withColumn("Male", col("Male").cast(IntegerType())) \
                               .withColumn("Total", col("Total").cast(IntegerType()))

  average_entries_by_gender = entriesgender.withColumn(
      'Avg_Female', entriesgender['Female'] / entriesgender['Total']
  ).withColumn(
      'Avg_Male', entriesgender['Male'] / entriesgender['Total']
  )
  ```

  Here, we cast columns to appropriate data types and calculate the average number of entries by gender for each discipline.

**3. Exporting Processed Data to Azure Synapse:**

After data transformation and analysis, the processed data is exported to Azure Synapse for further analysis and visualization. This can be done by writing the DataFrames back to Azure Data Lake Storage and then importing them into Synapse or using Synapse's built-in capabilities to directly query the data in the lake.

**4. Visualization and Reporting in Azure Synapse:**

With the data in Azure Synapse, users can create interactive dashboards and reports using Power BI or Synapse Studio. This integration provides a powerful way to visualize the insights derived from the data.

#### Conclusion

The integration of Azure Data Factory, Azure Databricks, and Azure Synapse Analytics provides a comprehensive solution for data integration, big data processing, and analytics. By leveraging these services, organizations can build scalable, efficient, and collaborative data pipelines to process and analyze vast amounts of data. The Tokyo Olympic data example demonstrates how these tools can be used in tandem to achieve seamless data workflows and generate valuable insights.
