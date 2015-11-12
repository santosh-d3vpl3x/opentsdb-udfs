# OpenTSDB Pig UDFs

[Applicable for 2.0 > OpenTSDB versions > 2.2]

OpenTSDB stores data in a specific way with predefined key structure and maintains reverse lookup for mappings.
The structure is very neatly explained in OpenTSDB [documentation](http://opentsdb.net/docs/build/html/user_guide/backends/hbase.htm "HBase schema documentation").

This structure can be leveraged to perform analysis of OpenTSDB data. As HBase is a part of Hadoop ecosystem, MapReduce can be used to accelerate the process.
Initial effort has been spent on getting the counts of combinations of metric name and tagk->tagv using Pig UDFs. 

## Getting Started
Project setup is fairly simple.
 
- Clone GIT repo.
- Build jar using maven.
- Upload jar to the HDFS in `\tmp`.
- Run pig scripts stored in `scripts` directory.

If you have changed width of UIDs used by OpenTSDB then you can configure this script to use them by utilizing either of :

1. change values of constants in com.opentsdb.tools.udf.utils.Common.Constants
2. in pig script, at the time of function definition, pass arguments in the constructor. it will look something like:`define generate_mapping com.opentsdb.tools.udf.MetricToTagsHash(4,5,6);` UDF will be initialized with 4 bytes width for metric uids, 5 bytes width for tag key uids and 6 bytes width for tag value uids.

These UDFs and pig scripts use defaults set in OpenTSDB by default. If you have made some changes then please make them here too. Otherwise, this script can return some garbage data.


**Caution:** The provided pig script scans entire HBase tsdb data. If you are interested in getting the counts for a single metric with some time limit, consider [putting a  criteria](https://pig.apache.org/docs/r0.9.1/api/org/apache/pig/backend/hadoop/hbase/HBaseStorage.html#HBaseStorage(java.lang.String, java.lang.String) "Limiting Data with Criteria in Pig") while loading data from HBase.