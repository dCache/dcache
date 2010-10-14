/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.util.debug;

import java.io.PrintStream;

/**
 * @deprecated Use Log4j API instead.
 */
public class Debug {

  public static int debugLevel = 0;
  public static PrintStream out = System.err;

  static {
    String lv = null;
    lv = System.getProperty("org.globus.debug.level");
    if (lv != null) {
      setDebugLevel(lv);
    } else {
      lv = System.getProperty("DEBUG_LEVEL");
      if (lv != null) setDebugLevel(lv);
    }
  }
  
  private static void setDebugLevel(String value) {
    Integer i = new Integer(value);
    debugLevel = i.intValue();
  }

  public static void println(String msg) {
    Debug.out.println(msg);
  }

  public static void print(String msg) {
    Debug.out.print(msg);
  }

  public static void error(String msg) {
    error(msg, null);
  }

  public static void error(String msg, Exception ex) {
    System.err.println(msg);
    if (debugLevel >= 3 && ex != null) ex.printStackTrace();
  }

  public static void debug(String msg) {
    debug(1, msg);
  }

  public static void debug(int level, String msg) {
    if (level <= debugLevel) Debug.out.println(msg);
  }

  public static void debug(int level, String header, String msg) {
    if (level <= debugLevel) {
      Debug.out.println(header + msg);
    }
  }

  public static void debug(int level, String msg, Exception e) {
    if (level <= debugLevel) {
      System.err.println(msg);
      if (level >= 3 && e != null) e.printStackTrace();
    }
  }

}
