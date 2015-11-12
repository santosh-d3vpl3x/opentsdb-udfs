package com.opentsdb.tools.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.opentsdb.tools.udf.utils.Common;

/**
 * @author Santosh Pingale
 * 
 *         UDF to convert input hex string to int.
 */
public class TsdUIDtoLong extends EvalFunc<Long> {

  @Override
  public Long exec(Tuple input) throws IOException {
    DataByteArray uidString = (DataByteArray) input.get(0);
    byte[] uidBytes = uidString.get();
    String uidHex = Common.hex(uidBytes);
    long uid = Common.hexToLong(uidHex);
    return uid;
  }
}
