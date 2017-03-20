/*
 * Modifications copyright (C) 2017 Microsoft
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.spark.perf.sql.hibench;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;

import java.io.IOException;
import java.util.Random;

public class HtmlCore {

  private static final Log log = LogFactory.getLog(HtmlCore.class.getName());

  private static final int maxUrlLength = 100;
  private static final int minUrlLength = 10;

  private static final double linkratio = 0.05;

  private static final int meanContentLen = 800;    // Guassian mean of content length
  private static final double varContentLen = 80;    // Guassian variance of content length
  private static final double gaussLowerLimit = -(meanContentLen / varContentLen);
  private static final double gaussUpperLimit = (Short.MAX_VALUE - meanContentLen) / varContentLen;

  private static final double epercent = 0.5;

  public static final String LINK_ZIPF_FILE_NAME = "linkzipf";

  private static Random randRandSeed;    // special rand to create random seeds
  private static Random randUrl, randPageGo, randElinks;

  public int slots;
  public long pages, slotpages, totalpages, outpages;
  public double zipfConstant;
  private ZipfianGenerator lzipf;

  HtmlCore(SparkConf workloadConf) throws IOException {
    pages = workloadConf.getLong("pages", 0);
    slotpages = workloadConf.getLong("slotpages", 0);
    zipfConstant = workloadConf.getDouble("zipfconstant", 0.99);
    slots = (int) Math.ceil((pages * 1.0 / slotpages));
    outpages = (long) Math.floor(pages * epercent);
    totalpages = pages + outpages;
    lzipf = new ZipfianGenerator(pages, zipfConstant);
  }

  public static long[] getPageRange(int slotId, int limit, int slotlimit) {
    long[] range = new long[2];
    range[0] = slotlimit * (slotId - 1);
    range[1] = range[0] + slotlimit;
    if (range[1] > limit) {
      range[1] = limit;
    }
    return range;
  }

  public void fireRandom(int rseed) {
    randRandSeed = new Random(rseed);

    randUrl = new Random(randRandSeed.nextLong());
    randElinks = new Random(randRandSeed.nextLong());
    randPageGo = new Random(randRandSeed.nextLong());
  }

  public int nextUrlLength() {
    return (int) Math.round(
            randPageGo.nextInt(maxUrlLength - minUrlLength + 1) + minUrlLength);
  }

  public int nextContentLength() {
    double gauss = 0;
    do {
      gauss = randPageGo.nextGaussian();
    } while ((gauss < gaussLowerLimit) || (gauss > gaussUpperLimit));

    return (int) Math.round(meanContentLen
            + varContentLen * gauss);
  }

  public long[] genPureLinkIds() {
    long[] links = new long[(int) Math.floor(linkratio * nextContentLength())];
    for (int i = 0; i < links.length; i++) {
      links[i] = lzipf.nextValue();
    }
    return links;
  }

}
