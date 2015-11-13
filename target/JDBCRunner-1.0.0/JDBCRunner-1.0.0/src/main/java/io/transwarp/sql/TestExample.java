package io.transwarp.sql;

import io.transwarp.constant.Constants;
import io.transwarp.sql.example.ExampleUtil;
import io.transwarp.sql.jdbc.DataBaseConnector;
import io.transwarp.sql.util.SqlConstants;
import io.transwarp.sql.util.TestUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Test with jdbc
 * @author xiliu
 *
 */
public class TestExample {
  static {
    Configuration.addDefaultResource("hbase-test.xml");
  }
  
  private Configuration conf = HBaseConfiguration.create();
  public AtomicLong count = new AtomicLong(0);
  public AtomicLong latency = new AtomicLong(0);
  private List<Worker> threads = new ArrayList<Worker>();
  private Counter counter;
  private SqlCreator sqlCreator = null;
  private static long boost = 0;
  
  private void runSql(Statement stat, String sql) {
    try {
      stat.execute(sql);
    } catch (SQLException e) {
       e.printStackTrace();
      // System.exit(-1);
    }
  }
  
  class Worker extends Thread {
    DataBaseConnector jdbc = null;
    public boolean stopped;
    
    public Worker(String host, int port) {
      // jdbc = new JDBCConnector(host, port);
      jdbc = TestUtil.getDBConnector(host, port, conf);
      System.err.println("jdbc connect succ.");
      this.stopped = false;
    }
    
    public void run() {
      System.out.println(conf.get("hbase.zookeeper.quorum"));
      Statement stat = jdbc.getStatement();
      
      try {
        stat.setFetchSize(1);
        runPreCmdIfNeed(stat, sqlCreator);
      } catch (SQLException e) {
        e.printStackTrace();
        return;
      }
      
      while (!stopped) {
        long pt = System.currentTimeMillis();
        String sql = sqlCreator.createSql();
        runSql(stat,sql);
        try {
            boolean flag = verifyResult(sql,stat.getResultSet());
            System.out.println(sql +" verify: "+ (flag==true ? "OK":"FAILURE"));
        } catch (SQLException e) {
          // e.printStackTrace();
        }
        long et = System.currentTimeMillis();
        count.addAndGet(1);
        latency.addAndGet(et - pt);
      }
      try {
        jdbc.close();
        System.err.println("jdbc close succ");
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }
  
  class Counter extends Thread {
    public boolean stopped;
    
    public Counter () {
      this.stopped = false;
    }
    
    public void run() {
      try {
        int interval = conf.getInt(Constants.INTERVAL_KEY,
            Constants.DEFALUT_INTERVAL) * 1000;

        long preCount = 0;
        long preLatency = 0;
        while (!stopped) {
          long v1 = count.get();
          long v2 = latency.get();
          System.out.println("count : "
              + (v1 - preCount)
              + ", avg latency : "
              + ((v1 - preCount) == 0 ? -1 : (v2 - preLatency)
                  / (v1 - preCount)));
          preCount = v1;
          preLatency = v2;
          Thread.sleep(interval);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
  
  class SqlSignalHandler implements SignalHandler {
    public void handle(Signal signal) {
      System.out.println("Signal handler called for signal " + signal);
      close();
    }

  }
  
  private void runPreCmdIfNeed(Statement stat, SqlCreator creator) throws SQLException {
    if (creator.needRunPreCmd()) {
      String condition = conf.get(SqlConstants.PRE_CONDITION_KEY, SqlConstants.DEFAULT_PRE_CONDITION);
      String[] conditions = condition.split(";");
      for (String s : conditions) {
        // run tp condition first
        stat.execute(s);
      }
    }
  }

  private void exampleInit() throws SQLException {
    int port = conf.getInt(SqlConstants.PORT_KEY, SqlConstants.DEFAULT_PORT);
    String[] hosts = getHosts(sqlCreator);
    DataBaseConnector jdbc = TestUtil.getDBConnector(hosts[0], port, conf);
    Statement stat = jdbc.getStatement();

    String drop_table_sql=ExampleUtil.dropSQL();
    runSql(stat, drop_table_sql);
    String create_table_sql = ExampleUtil.createSQL();
    runSql(stat, create_table_sql);
    for(int i=0;i<100;i++){
        String sql =ExampleUtil.insertSQL(i);
        System.out.println(sql);
        runSql(stat, sql);
    }
    jdbc.close();
  }

  private String[] getHosts(SqlCreator creator) {
    String[] hosts = null;
    if (creator.isTpSql()) {
      // choose tp hosts
      String tpHosts = conf.get(SqlConstants.TP_HOSTS_KEY);
      assert tpHosts != null : "must specify tp hosts for sql creator " + creator;
      hosts = tpHosts.split(";");
    } else {
      // choose ap hosts
      String apHosts = conf.get(SqlConstants.AP_HOSTS_KEY);
      assert apHosts != null : "must specify ap hosts for sql creator " + creator;
      hosts = apHosts.split(";");
    }
    return hosts;
  }
  
  public void close() {
    for (Worker w : threads) {
      w.stopped = true;
    }
    counter.stopped = true;
  }
  
  public void installSignalHander() {
    SqlSignalHandler handler = new SqlSignalHandler();
    Signal.handle(new Signal("TERM"), handler);
    Signal.handle(new Signal("INT"), handler);
  }
  
  public void start() throws IOException, SQLException, InterruptedException {

    TestUtil.printConf(conf);
    int threadnum = conf.getInt(Constants.THREAD_NUM_KEY, Constants.DEFAULT_THREAD_NUM);
    int port = conf.getInt(SqlConstants.PORT_KEY, SqlConstants.DEFAULT_PORT);

    sqlCreator = TestUtil.getExampleCreator(conf);
    if (sqlCreator == null) {
        return;
    }
    exampleInit();
    ExampleUtil.initSql();
    String[] hosts = getHosts(sqlCreator);
    TestUtil.initRandom();

    for (int i=0; i<threadnum; ++i) {
      threads.add(new Worker(hosts[i % hosts.length], port));
    }
    
    for (Worker w : threads) {
      w.start();
    }
    
    counter = new Counter();
    counter.start();
    
    for (Worker w : threads) {
      w.join();
    }
    counter.join();
  }


  public static void createDir(String outputdir){
     File outputDir = new File(outputdir);
     if (!outputDir.exists()) {
        outputDir.mkdirs();
     }
  }

  public static boolean  verifyResult(String sql,ResultSet rs){

      List<String> result = getResult(rs);
      List<String> answer = getAnswer(sql);
      Collections.sort(result);
      Collections.sort(answer);

//      if(result.equals(answer)){
//          return true;
//      }
//      System.out.println(sql+":  resultSize: "+result.size()+"; answerSize: "+answer.size());
     if(result.size() != answer.size()){
          return false;
      }
      for(int i=0;i<result.size();i++){
          if(!result.get(i).equals(answer.get(i))) {
              return false;
          }
      }
      return true;
  }

    public static List<String> getResult(ResultSet rs){

        StringBuffer sb = new StringBuffer();
        List<String> list = new ArrayList<String>();
        try {
            ResultSetMetaData resultSetMD = rs.getMetaData();
            int columnLength = resultSetMD.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnLength; i++) {
                    sb.append(rs.getString(i));
                    if (i != columnLength) {
                        sb.append("\t");
                    } else {
                        list.add(sb.toString());
                        sb = new StringBuffer();
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    public static List<String> getAnswer(String sql){

        List<String> list = new ArrayList<String>();
        Pattern pattern = Pattern.compile("'(.*?)'");
        Matcher matcher = pattern.matcher(sql);
        int idx = -1;
        if(matcher.find()){
            String str = matcher.group(1);
            if(str.startsWith("a0")){
                idx = Integer.parseInt(str.substring(2));
            }else{
                idx = Integer.parseInt(str.substring(1));
            }
        }
        if(idx > -1) {
            if (sql.indexOf(">") > 0) {
                for(int i=idx+1;i<100;i++){
                    String tmp="a"+String.format("%02d",i)+"\t"+(double)i+"\t"+(float)i+"\t"+i+"\t"+"d"+String.format("%02d",i);
                    list.add(tmp);
                }
            } else if (sql.indexOf("<") > 0) {
                for(int i=0;i<idx;i++){
                    String tmp="a"+String.format("%02d",i)+"\t"+(double)i+"\t"+(float)i+"\t"+i+"\t"+"d"+String.format("%02d",i);
                    list.add(tmp);
                }
            } else {
                String tmp="a"+String.format("%02d",idx)+"\t"+(double)idx+"\t"+(float)idx+"\t"+idx+"\t"+"d"+String.format("%02d",idx);
                list.add(tmp);
            }
        }
        return list;
    }


  public static void main(String args[]) throws IOException, NumberFormatException, InterruptedException, SQLException {
    TestExample runner = new TestExample();
    runner.installSignalHander();
    runner.start();
    System.err.println("Finished");
  }
  
}
