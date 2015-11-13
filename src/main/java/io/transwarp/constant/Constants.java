package io.transwarp.constant;

public class Constants {
  public final static String RUN_IN_TEST_MODE_KEY = "io.transwarp.runintestmode";
  public final static boolean DEFAULT_RUN_IN_TEST_MODE = false;
  
  public final static String THREAD_NUM_KEY = "io.transwarp.threadnum";
  public final static int DEFAULT_THREAD_NUM = 100;
  public final static String INTERVAL_KEY = "io.transwarp.interval";
  public final static int DEFALUT_INTERVAL = 1; // in seconds
  public final static String EXAMPLE_TEST = "io.transwarp.runexample";
  public final static boolean DEFAULT_EXAMPLE_TEST = false;
  public final static String KEYTAB_LOCALTION = "io.transwarp.user.keytab.location";
  public final static String DEFAULT_KEYTAB_LOCALTION = "/etc/inceptorsql1/hive.keytab";
}
