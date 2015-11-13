package io.transwarp.sql.util;

import io.transwarp.constant.Constants;
import io.transwarp.sql.PrecompileSqlCreator;
import io.transwarp.sql.SqlCreator;
import io.transwarp.sql.jdbc.DB2JDBCConnector;
import io.transwarp.sql.jdbc.DataBaseConnector;
import io.transwarp.sql.jdbc.JDBCConnector;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class TestUtil {
  
  private static Map<Integer, List<String>> parametersMap = new HashMap<Integer, List<String>>();
  
  private static final Class<?>[] EMPTY_ARRAY = new Class[]{};
  
  public static final Log LOG = LogFactory.getLog(TestUtil.class);
  
  private static Random r = new Random();
  public static int testBatch = 50;
  public static List<String> sqlList=new ArrayList<String>();
  
  public static void initRandom() {
    r.setSeed(System.currentTimeMillis());
  }
  
  private static void initListWithFile(String path, List<String> list) throws IOException {
    if (path == null || list == null) {
      return;
    }
    File file = new File(path);
    BufferedReader br = new BufferedReader(new FileReader(file));
    String line = null;
    while ((line = br.readLine()) != null) {
      list.add(line);
    }
    br.close();
  }
  
  public static String randomParameter(int index) {
    List<String> list = parametersMap.get(index);
    if (list != null && !list.isEmpty()) {
      return list.get(r.nextInt(list.size()));
    }
    return null;
  }
  
  public static void printConf(Configuration conf) {
    StringBuilder sb = new StringBuilder();
    sb.append("RUN_IN_TEST_MODE_KEY : ").append(conf.get(Constants.RUN_IN_TEST_MODE_KEY)).append("\n")
    .append("SQL_CREATOR_KEY : ").append(conf.get(SqlConstants.SQL_CREATOR_KEY)).append("\n")
    .append("THREAD_NUM_KEY : ").append(conf.getInt(Constants.THREAD_NUM_KEY, 0)).append("\n")
    .append("INTERVAL_KEY : ").append(conf.getInt(Constants.INTERVAL_KEY, 0)).append("\n")
    .append("TP_CONDITION_KEY : ").append(conf.get(SqlConstants.PRE_CONDITION_KEY)).append("\n")
    .append("TP_HOSTS_KEY : ").append(conf.get(SqlConstants.TP_HOSTS_KEY)).append("\n")
    .append("AP_HOSTS_KEY : ").append(conf.get(SqlConstants.AP_HOSTS_KEY)).append("\n");
    int posix = 0;
    while (true) {
      String key = SqlConstants.SQL_PARAMETERS_KEY + "." + posix;
      String file = conf.get(key);
      if (file == null) {
        break;
      } else {
        sb.append("Parameter ").append(posix).append(" : ").append(file).append("\n");
        posix++;
      }
    }
    System.out.println(sb.toString());
  }
  
  public static void initTestInput(Configuration conf) throws IOException {
    // init sql parameters
    int posix = 0;
    while (true) {
      String key = SqlConstants.SQL_PARAMETERS_KEY + "." + posix;
      String file = conf.get(key);
      if (file == null) {
        break;
      } else {
        List<String> value = new ArrayList<String>();
        initListWithFile(file, value);
        parametersMap.put(posix++, value);
      }
    }
    
    // set test batch
    int batch = conf.getInt(SqlConstants.TEST_BATCH_KEY, SqlConstants.DEFAULT_TEST_BATCH);
    testBatch = batch;
    assert testBatch >= 1 : "testBatch should set to at lease one.";
  }

  public static SqlCreator getSqlCreator(Configuration conf) {
    String className = conf.get(SqlConstants.SQL_CREATOR_KEY);
    if (className == null) {
      System.out.println("must set io.transwarp.sql.sqlcreator in conf");
      LOG.error("must set io.transwarp.sql.sqlcreator in conf.");
      return null;
    }
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    Class<?> clazz;
    try {
      clazz = Class.forName(className, true, classLoader);
      SqlCreator creator = (SqlCreator)newInstance(clazz);
      return creator;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
    
  }

    public static SqlCreator getExampleCreator(Configuration conf) {
        String className = "io.transwarp.sql.example.TEST$TEST1";
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class<?> clazz;
        try {
            clazz = Class.forName(className, true, classLoader);
            SqlCreator creator = (SqlCreator)newInstance(clazz);
            return creator;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

  public static PrecompileSqlCreator getPrecompileSqlCreator(Configuration conf) {
    String className = conf.get(SqlConstants.SQL_PRECOMPILE_CREATOR_KEY);
    if (className == null) {
      LOG.error("must set io.transwarp.sql.sqlprecompilecreator in conf.");
      return null;
    }
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    Class<?> clazz;
    try {
      clazz = Class.forName(className, true, classLoader);
      PrecompileSqlCreator creator = (PrecompileSqlCreator)newInstance(clazz);
      return creator;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
    
  }
  
  public static DataBaseConnector getDBConnector(String host, int port, Configuration conf) {
    String className = conf.get(SqlConstants.DB_CONNECTOR_KEY);
    if (className == null) {
      LOG.error("must set io.transwarp.sql.dbconnector in conf.");
      return null;
    }
    if (className.equalsIgnoreCase("transwarp")) {
      JDBCConnector connector = new JDBCConnector(host, port, conf);
      return connector;
    } else if (className.equalsIgnoreCase("db2")) {
      String user = conf.get(SqlConstants.DB2_USER);
      String passwd = conf.get(SqlConstants.DB2_PASSWD);
      String instance = conf.get(SqlConstants.DB2_INSTANCE);
      String url = "jdbc:db2://" + host + ":" + port + "/" + instance;
      LOG.error("connect url : " + url);
      DB2JDBCConnector connector = new DB2JDBCConnector(url, user, passwd);
      return connector;
    } else {
      LOG.error("dbconnetor only support transwarp or db2.");
      return null;
    }
    
  }
  
  private static <T> T newInstance(Class<T> clazz) 
      throws NoSuchMethodException, SecurityException, 
      InstantiationException, IllegalAccessException, 
      IllegalArgumentException, InvocationTargetException {
    Constructor<T> meth = clazz.getDeclaredConstructor(EMPTY_ARRAY);
    meth.setAccessible(true);
    return meth.newInstance();
  }


}
