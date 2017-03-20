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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Visit {
  private Random rand;
  private String delim = ",";
  private static String[] uagents, ccodes, skeys;
  private long dateRange;
  private Date date;
  private SimpleDateFormat dateForm;

  @SuppressWarnings("deprecation")
  Visit(String delim) throws IOException {
    rand = new Random();
    date = new Date();
    dateRange = Date.parse("Tue 1 May 2012 00:00:00");  // random date range
    dateForm = new SimpleDateFormat("yyyy-MM-dd");
    if (null != delim) {
      this.delim = delim;
    }
    try {
      initLock.lock();
      if (!initialized) {
        initAgents();
        initCountryCodes();
        initSearchKeys();
        initialized = true;
      }
      initLock.unlock();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static Lock initLock = new ReentrantLock();
  private static boolean initialized = false;

  private void initSearchKeys() throws ClassNotFoundException, IOException {
    int len = 99171;
    skeys = new String[len];
    InputStream newis = Class.forName("com.databricks.spark.sql.perf.sql.hibench.Visit").
            getResourceAsStream("/words");
    InputStreamReader isr = new InputStreamReader(newis);
    BufferedReader br = new BufferedReader(isr);
    String wd  = br.readLine();
    int idx = 0;
    while (wd != null) {
      skeys[idx++] = wd;
      wd = br.readLine();
    }
    br.close();
  }

  private void initCountryCodes() {
    Locale[] locales = Locale.getAvailableLocales();
    ccodes = new String[locales.length];
    int ccodesIdx = 0;
    for( Locale locale : locales ){
      String country = null, language = null;
      try {
        country = locale.getISO3Country();
        language = locale.getLanguage().toUpperCase();
      } catch (Exception e) {
        continue;
      }

      if (!"".equals(country) && !"".equals(language)) {
        String ccode = country + "," + country + "-" + language + "\n";
        ccodes[ccodesIdx++] = ccode;
      }
    }
  }

  private static String[] rawUAgents = {
          "0.0800 	Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)",
          "0.0300 	Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)",
          "0.1300 	Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",
          "0.0900 	Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.2)",
          "0.0700 	Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)",
          "0.0700 	Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
          "0.0400 	Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1)",
          "0.0150 	Mozilla/4.0 (compatible; MSIE 5.5; Windows NT 5.0)",
          "0.0080 	Mozilla/4.0 (compatible; MSIE 5.5; Windows 98; Win 9x 4.90)",
          "0.0600 	Mozilla/5.0 (Windows; U; Windows NT 5.2) AppleWebKit/525.13 (KHTML like Gecko) Chrome/xxx",
          "0.0300 	Mozilla/5.0 (Windows; U; Windows NT 5.2)AppleWebKit/525.13 (KHTML like Gecko) Version/3.1Safari/525.13",
          "0.0400 	Mozilla/5.0 (iPhone; U; CPU like Mac OS X)AppleWebKit/420.1 (KHTML like Gecko) Version/3.0 Mobile/4A93Safari/419.3",
          "0.0050 	iPhone 3.0: Mozilla/5.0 (iPhone; U; CPU iPhone OS 3_0 like Mac OS X; en-us) AppleWebKit/528.18 (KHTML like Gecko) Version/4.0 Mobile/7A341 Safari/528.16",
          "0.0050 	Mozilla/5.0 (Macintosh; U; PPC Mac OS X; en) AppleWebKit/125.2 (KHTML like Gecko) Safari/125.8",
          "0.0030		Mozilla/5.0 (Macintosh; U; PPC Mac OS X; en) AppleWebKit/125.2 (KHTML like Gecko) Safari/85.8",
          "0.0080 	Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.12)Gecko/20080219 Firefox/2.0.0.12	Navigator/9.0.0.6",
          "0.0070 	Mozilla/5.0 (Windows; U; Windows NT 5.2)Gecko/2008070208 Firefox/3.0.1",
          "0.0080 	Mozilla/5.0 (Windows; U; Windows NT 5.1)Gecko/20070309 Firefox/2.0.0.3",
          "0.0060 	Mozilla/5.0 (Windows; U; Windows NT 5.1)Gecko/20070803 Firefox/1.5.0.12",
          "0.0112 	Mozilla/5.0 (Windows; U; Windows NT 5.2) AppleWebKit/525.13 (KHTML like Gecko) Chrome/0.2.149.27 Safari/525.13",
          "0.0075 	Netscape 4.8 (Windows Vista): Mozilla/4.8 (Windows NT 6.0; U)",
          "0.0033 	Opera 9.2 (Windows Vista): Opera/9.20 (Windows NT 6.0; U; en)",
          "0.0028 	Opera 8.0 (Win 2000): Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0; en) Opera 8.0",
          "0.0035 	Opera 7.51 (Win XP): Opera/7.51 (Windows NT 5.1; U)",
          "0.0030 	Opera 7.5 (Win XP): Opera/7.50 (Windows XP; U)",
          "0.0020 	Opera 7.5 (Win ME): Opera/7.50 (Windows ME; U)",
          "0.0012 	Opera/9.27 (Windows NT 5.2; U; zh-cn)",
          "0.0048 	Opera/8.0 (Macintosh; PPC Mac OS X; U; en)",
          "0.0026 	Mozilla/5.0 (Macintosh; PPC Mac OS X; U; en)Opera 8.0",
          "0.0025 	Netscape 7.1 (Win 98): Mozilla/5.0 (Windows; U; Win98; en-US; rv:1.4) Gecko Netscape/7.1 (ax)",
          "0.0035 	Netscape 4.8 ( Win XP): Mozilla/4.8 (Windows NT 5.1; U)",
          "0.0010 	Netscape 3.01 gold (Win 95): Mozilla/3.01Gold (Win95; I)",
          "0.0020 	Netscape 2.02 (Win 95): Mozilla/2.02E (Win95; U)",
          "0.2441		***"
  };

  private void initAgents() {
    int numSourceUAgents = 2000;
    uagents = new String[rawUAgents.length];
    for (int i = 0; i < rawUAgents.length; i++) {
      String[] pair = rawUAgents[i].split("\t");
      int num = (int) Math.round(Double.parseDouble(pair[0]) * numSourceUAgents);
      String content = "";
      for (int j = 0; j < num; j++) {
        if ("***".equals(pair[pair.length - 1])) {
          content = content + nextSeedAgent() + "\n";
        } else {
          content = content + pair[pair.length - 1] + "\n";
        }
      }
      uagents[i] = content.trim();
    }
  }

  private static String nextSeedAgent() {
    Random rand = new Random(11);
    int len = rand.nextInt(20) + 5;
    char[] sagent = new char[len + 4];

    sagent[0] = (char) (rand.nextInt(26) + 'A');
    for (int i=1; i<len; i++) {
      sagent[i] = (char) (rand.nextInt(26) + 'a');
    }
    sagent[len++] = '/';
    sagent[len++] = (char) (rand.nextInt(9) + (int) ('0'));
    sagent[len++] = '.';
    sagent[len++] = (char) (rand.nextInt(9) + (int) ('0'));

    return new String(sagent);
  }

  private String nextCountryCode() {
    return ccodes[rand.nextInt(ccodes.length)];
  }

  private String nextUserAgent() {
    return uagents[rand.nextInt(uagents.length)];
  }

  private String nextSearchKey() {
    return skeys[rand.nextInt(skeys.length)];
  }

  private String nextTimeDuration() {
    return Integer.toString(rand.nextInt(10) + 1);
  }

  private String nextIp() {
    return Integer.toString(rand.nextInt(254) + 1)
            + "." + Integer.toString(rand.nextInt(255))
            + "." + Integer.toString(rand.nextInt(255))
            + "." + Integer.toString(rand.nextInt(254) + 1);
  }

  private String nextDate() {
    date.setTime((long) Math.floor(rand.nextDouble() * dateRange));
    return dateForm.format(date);
  }

  private String nextProfit() {
    return Float.toString(rand.nextFloat());
  }

  /***
   * set the randseed of random generator
   * @param randSeed
   */
  public void fireRandom(int randSeed) {
    rand.setSeed(randSeed);
  }

  public UserVisitRecord nextAccess(String url) {
    return new UserVisitRecord(nextIp(), url, nextDate(), nextProfit(),
            nextUserAgent(), nextCountryCode(), nextSearchKey(), nextTimeDuration());
  }
}
