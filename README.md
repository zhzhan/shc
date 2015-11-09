# Spark-HBase Connector

Spark-HBase Connector is a library to support Spark accessing HBase table as external data source or sink. With it, user can operate HBase with Spark-SQL on data frame level. 

With the data frame support, the lib leverages all the optimization techniques in catalyst, and achieves data locality, partition pruning, predicate pushdown, Scanning and BulkGet, etc. 

## Architecture
The connector treats the scan and get similary. On the high level, the driver processes the query, aggregate scans and gets by the region server, and generagte tasks per region server. The task will be sent to the executors colocated with region server. The scans and BulkGets are performed in the executors in parallel to achieve better data locality and concurrency.

### Scan
Instead of operating on TableInputFormat, the connector operates on Region Sever directly by driver retrieving meta data and constructing tasks. 

The reason is that the TableInputFormat only accepts one Scan at a time, and the scan has to be continuous. If the user query consists of a big number of non-overlapping scan ranges, the same number of scans have to be constructed resulting in the same number of Spark tasks. If the tasks is larger than the executor cores in the region server, tasks may not be able to be allocated to the executors with data locality. In addition, big number of tasks will increase the scheduling and task sedes overhead.

This connector constructs all scans at the driver side, and aggregate all of the scans by the region server in one task. The scans in one task will execute in parallel in executors. By this way, the maximum number of tasks is no larger than the number of regions of a table to achieve better data locality.

### BulkGet
Similar to scans, the driver side also aggregates Gets based on the range by each region server into tasks together with scans. Implementation-wise, scan and bulkget follows the same logic. In other words, one task may consists of multiple scans and BulkGets.

## Catalog
For each table, a catalog has to be provided,  which includes the row key, and the columns with data type with predefined column families, and defines the mapping between hbase column and table schema. The catalog is user defined json format.

## Datatype conversion
Java primitive types is supported. In the future, other data types will be supported, which relies on user specified sedes. Take avro as an example. User defined sedes will be responsible to convert byte array to avro object, and connector will be responsible to convert avro object to catalyst supported datatypes. 

Note that if user want dataframe to only handle byte array, the binary type can be specified. Then user can get the catalyst row with each column as a byte array. User can further deserialize it with customized deserializer, or operate on the RDD of the data frame directly.

## Solve the conflicts of Catalyst datatype order and HBase byte array order
The libary automatically handle the orderness between the conflicts between the java data type (Short, Integer, Long, Float, Double, String, etc) and HBase bytearray order, which means as long as the format saved in HBase is consistent with the Java native data format, the libary will be able to take care of pruning and comparison instead of relying on a specific import tools.

## Data locality
When the spark worker node co-located with hbase region servers, data locality is achieved by identifying the region server location, and co-locate the executor with the region server. Each executor will only perform Scan/BulkGet on the part of the data that co-locates on the same host. Because the number of tasks is no larger than the region server, there is a good chance to achieve data locality.

## Predicate pushdown
The lib use existing standard HBase filter provided by HBase and does not operate on the coprocessor. 

## Partition Pruning
By extracting the row key from the predicates, we split the scan/BulkGet into multiple non-overlapping ranges, only the region servers that have the requested data will perform scans/BulkGets. Currently, the partition pruning is performed on the first dimension of the row keys. Note that the WHERE conditions need to be defined carefully. Otherwise, the result scanning may includes a region larger than user expected. For example, following condition will result in a full scan of the table (rowkey1 is the first dimension of the rowkey, and column is a regular hbase column).
         WHERE rowkey1 > "abc" OR column = "xyz"

## Scanning and BulkGet
Both are exposed to users by specifying WHERE CLAUSE, e.g., where column > x and column < y for scan and where column = x for get. All the operations are performed in the executors, and driver only constructs these operations. Internally we will convert them to scan or get or combination of both, which return Iterator[Row] to catalyst engine. 

## Dataframe Write Support
The connector supports data frame persist to the HBase table directly.

## Composite Key Support
The connector allows the HBase table consisting of mulitple keys. Currently, the partition prunning happens on the first dimension only. 

##Application API/Usage
Following the the examples how to write and query a HBase table. Please refer to https://github.com/hortonworks/shc/blob/master/src/test/scala/org/apache/spark/sql/DefaultSourceSuite.scala for details.

###Compile

    mvn package -DskipTests

###Running Tests and Examples
Due to hbase pseducluster setup, currently the tests to be run one by one. 

    mvn -DwildcardSuites=org.apache.spark.sql.DefaultSourceSuite test

The following also illustrate how to run the example in real hbase cluster. You need to provide the hbase-site.xml and related hbase jars. It may subject to change based on your specific cluster configuration.

        ./bin/spark-submit  --class org.apache.spark.sql.execution.datasources.hbase.examples.HBaseSource --master yarn-client     --num-executors 2     --driver-memory 512m     --executor-memory 512m     --executor-cores 1   --jars  /usr/hdp/current/hbase-client/lib/htrace-core-3.1.0-incubating.jar,/usr/hdp/current/hbase-client/lib/hbase-client.jar,/usr/hdp/current/hbase-client/lib/hbase-common.jar,/usr/hdp/current/hbase-client/lib/hbase-server.jar,/usr/hdp/current/hbase-client/lib/guava-12.0.1.jar,/usr/hdp/current/hbase-client/lib/hbase-protocol.jar,/usr/hdp/current/hbase-client/lib/htrace-core-3.1.0-incubating.jar  --files conf/hbase-site.xml /usr/hdp/current/spark-client/lib/hbase-spark-connector-1.0.0.jar


### Defined the HBase catalog

    def catalog = s"""{
            |"table":{"namespace":"default", "name":"table1"},
            |"rowkey":"key",
            |"columns":{
              |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
              |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
              |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
              |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
              |"col4":{"cf":"cf4", "col":"col4", "type":"int"},
              |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
              |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
              |"col7":{"cf":"cf7", "col":"col7", "type":"string"},
              |"col8":{"cf":"cf8", "col":"col8", "type":"tinyint"}
            |}
          |}""".stripMargin
         
The above defines a schema for a HBase table with name as table1, row key as key and a number of columns (col1-col8). Note that the rowkey also has to be defined in details as a column (col0), which has a specific cf (rowkey).

### Write to HBase table to populate data.

    sc.parallelize(data).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
      
Given a data frame with specified schema, above will create an HBase table with 5 regions and save the data frame inside. Note that if HBaseTableCatalog.newTable is not specified, the table has to be pre-created.

### Perform data frame operation on top of HBase table

    def withCatalog(cat: String): DataFrame = {
      sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog->cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    }
  
#### Complicated query

    val df = withCatalog(catalog)
    val s = df.filter((($"col0" <= "row050" && $"col0" > "row040") ||
      $"col0" === "row005" ||
      $"col0" === "row020" ||
      $"col0" ===  "r20" ||
      $"col0" <= "row005") &&
      ($"col4" === 1 ||
      $"col4" === 42))
      .select("col0", "col1", "col4")
    s.show
    
#### SQL support

    // Load the dataframe
    val df = withCatalog(catalog)
    //SQL example
    df.registerTempTable("table")
    sqlContext.sql("select count(col1) from table").show
    
#### TODO:

    val complex = s"""MAP<int, struct<varchar:string>>"""
    val schema =
      s"""{"namespace": "example.avro",
         |   "type": "record",      "name": "User",
         |    "fields": [      {"name": "name", "type": "string"},
         |      {"name": "favorite_number",  "type": ["int", "null"]},
         |        {"name": "favorite_color", "type": ["string", "null"]}      ]    }""".stripMargin
    val catalog = s"""{
            |"table":{"namespace":"default", "name":"htable"},
            |"rowkey":"key1:key2",
            |"columns":{
              |"col1":{"cf":"rowkey", "col":"key1", "type":"binary"},
              |"col2":{"cf":"rowkey", "col":"key2", "type":"double"},
              |"col3":{"cf":"cf1", "col":"col1", "avro":"schema1"},
              |"col4":{"cf":"cf1", "col":"col2", "type":"string"},
              |"col5":{"cf":"cf1", "col":"col3", "type":"double",        "sedes":"org.apache.spark.sql.execution.datasources.hbase.DoubleSedes"},
              |"col6":{"cf":"cf1", "col":"col4", "type":"$complex"}
            |}
          |}""".stripMargin
       
    val df = sqlContext.read.options(Map("schema1"->schema, HBaseTableCatalog.tableCatalog->catalog)).format("org.apache.spark.sql.execution.datasources.hbase").load()
    df.write.options(Map("schema1"->schema, HBaseTableCatalog.tableCatalog->catalog)).format("org.apache.spark.sql.execution.datasources.hbase").save()
          

Above illustrates our next step, which includes composite key support, complex data types, support of customerized sedes and avro. Note that although all the major pieces are included in the current code base, but it is not well tested yet.

