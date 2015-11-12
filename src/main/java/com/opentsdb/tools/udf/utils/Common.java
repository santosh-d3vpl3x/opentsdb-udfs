package com.opentsdb.tools.udf.utils;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.commons.codec.binary.Hex;

/**
 * @author Santosh Pingale
 * 
 *         A static class Contains general methods used by UDFs.
 *
 */
public class Common {

  private Common() {
  }

  /**
   * @param inputButes
   *          bytes to be converted to a hex string
   * @return hexString hex string obtained from input bytes
   * @throws IOException
   */
  public static String hex(byte[] inputButes) throws IOException {
    String hexString = Hex.encodeHexString(inputButes);
    return hexString;
  }

  /**
   * This method converts any hex string to long value.
   * 
   * @param hexString
   *          Hex for which long value is to be obtained
   * @return longHex long value obtained from hex
   */
  public static long hexToLong(String hexString) {
    long longHex = Long.parseLong(hexString, 16);
    return longHex;
  }

  /**
   * Converts hex string of OpenTSDB tag key, value UID to a Tree Map of <Long,Long> UIDs. All the
   * keys are sorted for future consistency.
   * 
   * @param tagHex
   *          Tag Key, value pair Hex
   * @return tags a java.util.TreeMap<Long,Long> which contains storted tag key, sorted tag value.
   */
  public static TreeMap<Long, Long> generateTagMap(String tagHex, int mWdth, int tkWdth, int tvWdth) {
    TreeMap<Long, Long> tags = new TreeMap<Long, Long>();
    int tagWidth = tkWdth + tvWdth;
    for (int i = 0; i < tagHex.length(); i = i + (tagWidth * 2)) {
      int tagKeyStartOffset = i;
      int tagKeyEndOffset = i + tkWdth * 2;
      String tagKeyHex = tagHex.substring(tagKeyStartOffset, tagKeyEndOffset);
      long tagKeyID = Common.hexToLong(tagKeyHex);

      int tagValStartOffset = i + tkWdth * 2;
      int tagValEndOffset = i + (tagWidth * 2);
      String tagValHex = tagHex.substring(tagValStartOffset, tagValEndOffset);
      long tagValID = Common.hexToLong(tagValHex);

      tags.put(tagKeyID, tagValID);
    }
    return tags;
  }

  /**
   * @author Santosh Pingale
   * 
   *         Constants used by UDFs. It also contains OpenTSDB default lengths for version 2.1.0.
   *         Constant values should be changed if you have customized the metrics.
   * 
   */
  public static class Constants {
    /**
     * Metric UID width in bytes specified for OpenTSDB. Check configurations of OpenTSDB property
     * 
     * <pre>
     * tsd.storage.uid.width.metric
     * </pre>
     * 
     * .
     */
    public static final byte M_UID_WIDTH = 3;

    /**
     * Tag Key UID width in bytes specified for OpenTSDB. Check configurations of OpenTSDB property
     * 
     * <pre>
     * tsd.storage.uid.width.tagk
     * </pre>
     * 
     * .
     */
    public static final byte TK_UID_WIDTH = 3;

    /**
     * Tag Value UID width in bytes specified for OpenTSDB. Check configurations of OpenTSDB
     * property
     * 
     * <pre>
     * tsd.storage.uid.width.tagv
     * </pre>
     * 
     * .
     */
    public static final byte TV_UID_WIDTH = 3;

    /**
     * Timestamp width in bytes specified for OpenTSDB.
     */
    public static final byte TIMESTAMP_WIDTH = 4;
  }
}
