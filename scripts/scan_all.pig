set pig.exec.nocombiner true;
set mapreduce.child.java.opts -Xmx2048m;
set mapreduce.map.memory.mb 3072;

register 'hdfs:///tmp/OpenTSDBCardinailty-0.0.1-jar-with-dependencies.jar'

define generate_mapping com.opentsdb.tools.udf.MetricToTagsHash;
define uidToLong com.opentsdb.tools.udf.TSDUIDtoLong;

TSD_METRICS     = LOAD 'hbase://tsdb' using  org.apache.pig.backend.hadoop.hbase.HBaseStorage('t', '-caster=HBaseBinaryConverter -loadKey=true') AS (key);
TRANSFORMED_DATA = FOREACH TSD_METRICS GENERATE flatten(generate_mapping(key)) as (metric_id:long, tag_hash:int);
TRANSFORMED_DATA_GROUPED = GROUP TRANSFORMED_DATA BY metric_id;
FINAL_COUNTS_BY_METRIC_ID = FOREACH TRANSFORMED_DATA_GROUPED{
	UNIQUE_TAGS = DISTINCT TRANSFORMED_DATA.tag_hash;
	generate group as metric_id, COUNT(UNIQUE_TAGS) as counts;
};

TSD_METRIC_UID_MAPPING = LOAD 'hbase://tsdb-uid' using  org.apache.pig.backend.hadoop.hbase.HBaseStorage('id:metrics', '-caster=HBaseBinaryConverter -loadKey=true') AS (metric_name,value);
TSD_METRIC_UID_MAPPING_TRANSFORMED = FOREACH TSD_METRIC_UID_MAPPING GENERATE metric_name, uidToLong(value) as metric_id;

FINAL_COUNTS_BY_METRIC_NAME_JOINED = JOIN FINAL_COUNTS_BY_METRIC_ID BY metric_id LEFT OUTER, TSD_METRIC_UID_MAPPING_TRANSFORMED BY metric_id; 
FINAL_COUNTS = FOREACH FINAL_COUNTS_BY_METRIC_NAME_JOINED GENERATE FINAL_COUNTS_BY_METRIC_ID::metric_id as metric_id,  TSD_METRIC_UID_MAPPING_TRANSFORMED::metric_name as metric_name, FINAL_COUNTS_BY_METRIC_ID::counts as counts;
FINAL_COUNTS_SORTED = ORDER FINAL_COUNTS BY counts DESC;

store FINAL_COUNTS_SORTED into '/tmp/final_metric_count' using PigStorage('\t');