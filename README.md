Step By Step Guide to Implement Solution with Glue - Lake Formation - Iceberg.


Guide to use Iceberg Connector( Shown in the Glue Connector usage details ) : 
    Please subscribe to the product from AWS Marketplace and Activate the Glue connector from AWS Glue Studio.

    The Iceberg connector enables you to access iceberg tables from your Glue ETL jobs. In particular, you can operate DDLs, read/write data, time-travels and streaming writes for the Iceberg sources. Further details about the Apache Iceberg, please see https://iceberg.apache.org/.

    Using the Apache Iceberg Connector

    Here's the steps to set up the Iceberg connector. For more details about using Iceberg on AWS, please refer to https://iceberg.apache.org/aws/.

    Required Spark configuration

    Before using this connector, you need to set the following Spark configuration for communication between this connector and the Glue Data Catalog.

    You can set the configuration on the job parameter in your Glue job or on the scripts in your Glue job. Please note that the configuration related to DynamoDB and spark.sql.extensions are optional, however those are recommended for accessing full operations of Iceberg.

    Set on the job parameter

        Key: --conf
        Value: spark.sql.catalog.<like AwsDataCatalog in Athena>=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.<like AwsDataCatalog in Athena>.warehouse=<S3_PATH> --conf spark.sql.catalog.<like AwsDataCatalog in Athena>.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.<like AwsDataCatalog in Athena>.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.<like AwsDataCatalog in Athena>.lock-impl=org.apache.iceberg.aws.glue.DynamoLockManager --conf spark.sql.catalog.<like AwsDataCatalog in Athena>.lock.table=<DynamoDB_TABLE_NAME> --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions


    Set on the Glue job script:

    from pyspark.conf import SparkConf
    conf = SparkConf()
    conf.set("spark.sql.catalog.<like AwsDataCatalog in Athena>", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.<like AwsDataCatalog in Athena>.warehouse", "<S3_PATH>")
    conf.set("spark.sql.catalog.<like AwsDataCatalog in Athena>.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    conf.set("spark.sql.catalog.<like AwsDataCatalog in Athena>.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    conf.set("spark.sql.catalog.<like AwsDataCatalog in Athena>.lock-impl", "org.apache.iceberg.aws.glue.DynamoLockManager")
    conf.set("spark.sql.catalog.<like AwsDataCatalog in Athena>.lock.table", "<DynamoDB_TABLE_NAME>")
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

    Connector options

    You can pass the following options to the connector.

        path - An Iceberg table name such as <like AwsDataCatalog in Athena>.<database>.<table>.
        as-timestamp-of or snapshotid (optional, set only for read) - You can operate "Time travel" queries by setting this parameter. Please see the document about the time-travel.


    In addition to the DynamicFrame with options, you can access the Iceberg table with DataFrames or SparkSQL. To do operations on your Iceberg table from Glue jobs, please see https://iceberg.apache.org/getting-started/.

    Current limitation
    ALTER TABLE ... RENAME TO is not available in the current version of connector.

    Also refer to :
        https://www.dremio.com/resources/tutorials/getting-started-with-apache-iceberg-using-aws-glue-and-dremio/
        https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html


# ---------------------------------

Lake Formation Steps 
    Register the S3 location in LF
        Register location
            S3 : s3://<Bucket Name>/<prefix-subdir>/output 
            IAM role : As is 

    Grant permissoin to Admin and <my-lab-role> 

    Create the database in LF 
        Make sure to have : 
                Location - optional : s3://<Bucket Name>/<prefix-subdir>/temp

    Create workgroup AmazonAthenaIcebergPreview in Athena and then run 

            CREATE TABLE my_lab_verana_iceberg_cust_table (
              customer_id int,
              first_name string,
              last_name string,
              city string,
              country string,
              eff_start_date date,
              eff_end_date date,
              is_current string,
              lastopflag string
              ) 
            PARTITIONED BY (customer_id, bucket(16,customer_id)) 
            LOCATION 's3://<Bucket Name>/<prefix-subdir>/output/' 
            TBLPROPERTIES (
              'table_type'='ICEBERG',
              'format'='parquet',
              'compaction_bin_pack_target_file_size_bytes'='536870912' 
            )
            ;

    Give Table access to Admin and <my-lab-role> in LF


# ---------------------------------

Implementing steps using Glue Studio 

Using Visual :
    Source : S3 
    Target : Iceberg Connector
                add to connection option 
                    path = <like AwsDataCatalog in Athena>.iceberg_database.my_lab_verana_iceberg_cust_table


In Job Details : 
    Job Name my-lab-<prefix-subdir>-job-1
    IAM Role : <my-lab-role>
    Glue version : Glue3.0 Spark3.1 Scala 2 Python 3
    Book Mark : Enaable 
    Current connections : my-lab-<prefix-subdir>-conn( MARKETPLACE )
    Job parameter ( Here Dynamo Table is not included to control the locks . If used then the add the lock table as shown )
        Key : --conf 
            Value : spark.sql.catalog.<like AwsDataCatalog in Athena>=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.<like AwsDataCatalog in Athena>.warehouse=s3://<Bucket Name>/<prefix-subdir>/output/ --conf spark.sql.catalog.<like AwsDataCatalog in Athena>.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.<like AwsDataCatalog in Athena>.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions


  
  
# ---------------------------------

  Script is : 

        import sys
        from awsglue.transforms import *
        from awsglue.utils import getResolvedOptions
        from pyspark.context import SparkContext
        from awsglue.context import GlueContext
        from awsglue.job import Job

        args = getResolvedOptions(sys.argv, ["JOB_NAME"])
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(args["JOB_NAME"], args)

        # Script generated for node S3 bucket
        S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
            format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
            connection_type="s3",
            format="csv",
            connection_options={
                "paths": ["s3://<Bucket Name>/<prefix-subdir>/input/cust_sample_data.csv"],
                "recurse": True,
            },
            transformation_ctx="S3bucket_node1",
        )

        # Script generated for node ApplyMapping
        ApplyMapping_node2 = ApplyMapping.apply(
            frame=S3bucket_node1,
            mappings=[
                ("customer_id", "long", "customer_id", "int"),
                ("first_name", "string", "first_name", "string"),
                ("last_name", "string", "last_name", "string"),
                ("city", "string", "city", "string"),
                ("country", "string", "country", "string"),
                ("eff_start_date", "string", "eff_start_date", "date"),
                ("eff_end_date", "string", "eff_end_date", "date"),
                ("is_current", "string", "is_current", "string"),
                ("lastopflag", "string", "lastopflag", "string"),
            ],
            transformation_ctx="ApplyMapping_node2",
        )

        # Script generated for node Iceberg Connector for Glue 3.0
        IcebergConnectorforGlue30_node3 = glueContext.write_dynamic_frame.from_options(
            frame=ApplyMapping_node2,
            connection_type="marketplace.spark",
            connection_options={
                "path": "<like AwsDataCatalog in Athena>.iceberg_database.my_lab_verana_iceberg_cust_table",
                "connectionName": "my-lab-<prefix-subdir>-conn",
            },
            transformation_ctx="IcebergConnectorforGlue30_node3",
        )

        job.commit()
