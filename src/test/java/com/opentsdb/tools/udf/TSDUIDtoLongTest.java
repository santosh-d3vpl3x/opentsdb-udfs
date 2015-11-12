package com.opentsdb.tools.udf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import junit.framework.TestCase;

import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import com.opentsdb.tools.udf.TsdUIDtoLong;

public class TSDUIDtoLongTest extends TestCase {

  @Test
  public void testExecTuple() {
    InputStream in = getClass().getResourceAsStream("/tsdb_uid-inputs");
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    String readLine = null;
    try {
      while ((readLine = br.readLine()) != null) {
        TsdUIDtoLong sba = new TsdUIDtoLong();
        Tuple input = TupleFactory.getInstance().newTuple(1);
        DataByteArray dba = new DataByteArray();
        String[] split = readLine.split("-->");
        String metric = split[0];
        String expectedVal = split[1];
        dba.append(metric.getBytes());
        input.set(0, dba);
        Long id = sba.exec(input);
        assertTrue("Expected " + expectedVal + ",got " + id, id == Long.parseLong(expectedVal));
      }
    } catch (IOException e) {
      System.out.println("Something went wrong");
      e.printStackTrace();
      fail("Something is wrong with file, make sure metric-inputs exists in classpath");
    } finally {
      try {
        br.close();
      } catch (IOException e) {
        System.out.println("Something went wrong. Not sure if it was a failure.");
        e.printStackTrace();
        // fail("Could not close file.");
      }
    }

  }

}
