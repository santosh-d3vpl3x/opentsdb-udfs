package com.mmt.dpt.pig.udf.reporting;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.codehaus.jackson.map.ObjectMapper;

import com.mmt.dpt.utils.Common;
import com.mmt.dpt.utils.Common.Constants;

/**
 * @author Santosh Pingale
 *
 *         UDF responsible for decoding tsdb key. Generates Hash of the values. Getting hash is
 *         optimizations to make sure we do not run out of memory.
 */
public class MetricToTagsHash extends EvalFunc<Tuple> {

  ObjectMapper objMapper = new ObjectMapper();

  int mWdth = Constants.M_UID_WIDTH;

  int tkWdth = Constants.TK_UID_WIDTH;

  int tvWdth = Constants.TV_UID_WIDTH;

  public MetricToTagsHash(int mWdth, int tkWdth, int tvWdth) {
    super();
    this.mWdth = mWdth;
    this.tkWdth = tkWdth;
    this.tvWdth = tvWdth;
  }
  
  

  public MetricToTagsHash() {
    super();
  }



  @Override
  public Tuple exec(Tuple input) throws IOException {
    try {

      DataByteArray inputKey = (DataByteArray) input.get(0);

      byte[] keyArray = inputKey.get();

      String keyHex = Common.hex(keyArray);

      String metricIDHex = keyHex.substring(0, mWdth * 2);
      long metricID = Common.hexToLong(metricIDHex);

      int tagsStartOffset = mWdth * 2 + Constants.TIMESTAMP_WIDTH * 2;
      String tagPairHex = keyHex.substring(tagsStartOffset, keyHex.length());

      TreeMap<Long, Long> tags = Common.generateTagMap(tagPairHex, mWdth, tkWdth, tvWdth);

      int tagsHash = objMapper.writeValueAsString(tags).hashCode();

      Tuple output = TupleFactory.getInstance().newTuple(2);
      output.set(0, metricID);
      output.set(1, tagsHash);
      return output;
    } catch (Exception ex) {
      log.error("Something went wrong!", ex);
      return null;
    }
  }

  @Override
  public Schema outputSchema(Schema input) {
    try {
      Schema tupleSchema = new Schema();
      tupleSchema.add(input.getField(0));
      tupleSchema.add(input.getField(1));
      String schemaName = getSchemaName(this.getClass().getName().toLowerCase(), input);
      return new Schema(new FieldSchema(schemaName, tupleSchema, DataType.TUPLE));
    } catch (Exception e) {
      return null;
    }
  }

}
