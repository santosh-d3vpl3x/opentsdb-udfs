package com.mmt.dpt.pig.udf.reporting;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import junit.framework.TestCase;

import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class TestMetricToTags extends TestCase {

  @Test
  public void testExec() {
    InputStream in = getClass().getResourceAsStream("/metric-inputs");
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    String readLine = null;
    try {
      while ((readLine = br.readLine()) != null) {
        MetricToTags sba = new MetricToTags();
        Tuple input = TupleFactory.getInstance().newTuple(1);
        DataByteArray dba = new DataByteArray();
        String[] split = readLine.split("-->");
        String metric = split[0];
        String[] expectedVal = split[1].split("\\|");
        dba.append(metric.getBytes());
        input.set(0, dba);
        Tuple t = sba.exec(input);
        assertTrue("Failed to Match Key", (Long) t.get(0) == Long.parseLong(expectedVal[0]));
        assertTrue("Failed to Match Value", t.get(1).equals(expectedVal[1]));
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
