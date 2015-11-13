package io.transwarp.sql.util;

public class SqlConstants {
  private final static String prefix = "io.transwarp.sql."; 
  
  public final static String FETCH_RESULT = prefix + "fetchresult";
  public final static String BOOST = prefix + "boost";
  public final static String SQL_CREATOR_KEY = prefix + "sqlcreator";
  public final static String SQL_PRECOMPILE_CREATOR_KEY = prefix + "precompilesqlcreator";
  public final static String DB_CONNECTOR_KEY = prefix + "dbconnector";
  public final static String DB2_USER = prefix + "user";
  public final static String DB2_PASSWD = prefix + "passwd";
  public final static String DB2_INSTANCE = prefix + "instance";
  public final static String PRE_CONDITION_KEY = prefix + "precondition";
  public final static String DEFAULT_PRE_CONDITION = "set ngmr.exec.mode=local;set ngmr.metacache=true";
  public final static String TP_HOSTS_KEY = prefix + "tp.hosts";
  public final static String AP_HOSTS_KEY = prefix + "ap.hosts";
  public final static String PORT_KEY = prefix + "port";
  public final static int DEFAULT_PORT = 10000;
  public final static String DATE_RANGE_KEY = prefix + "daterange"; // the delta between start year and end year when random date
  public final static int DEFAULT_DATE_RANGE = 1;
  public final static String HIVE_SERVER= prefix + "hiveServer";
  public final static String PRINCIPAL= prefix + "principal";
  public final static String SQLOUTPUT = prefix + "outputdir";
  public final static String DEFAULT_SQLOUTPUT = "/tmp";

  
  public final static String SQL_PARAMETERS_KEY = prefix + "parameters";
  
  public final static String TEST_BATCH_KEY = prefix + "batch";
  public final static int DEFAULT_TEST_BATCH = 50;
}
