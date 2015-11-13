package io.transwarp.sql;

import io.transwarp.constant.Constants;
import io.transwarp.sql.jdbc.DataBaseConnector;
import io.transwarp.sql.util.SqlConstants;
import io.transwarp.sql.util.TestUtil;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * Test with jdbc
 * @author xiliu
 *
 */
public class TestSQLUsePrecompile {
  static {
    Configuration.addDefaultResource("hbase-test.xml");
  }
  
  private Configuration conf = HBaseConfiguration.create();
  public AtomicLong count = new AtomicLong(0);
  public AtomicLong latency = new AtomicLong(0);
  private List<Worker> threads = new ArrayList<Worker>();
  private Counter counter;
  private PrecompileSqlCreator sqlCreator = null;
  
  class Worker extends Thread {
    DataBaseConnector jdbc = null;
    public boolean stopped;
    boolean fetchResult;
    int myCount = 0;
    
    public Worker(String host, int port, boolean fetchResult) {
      // jdbc = new JDBCConnector(host, port);
      jdbc = TestUtil.getDBConnector(host, port, conf);
      System.err.println("jdbc connect succ.");
      this.stopped = false;
      this.fetchResult = fetchResult;
    }
    
    public void run() {
      PreparedStatement stat = jdbc.prepareStatement(sqlCreator.sqlTemplate());
      try {
        stat.setFetchSize(1);
        runPreCmdIfNeed(stat, sqlCreator);
      } catch (SQLException e) {
        e.printStackTrace();
        return;
      }
      /*
      try {
        stat.execute("set ngmr.exec.mode=local");
      } catch (SQLException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      */
      
      while (!stopped) {
        long pt = System.currentTimeMillis();
        try {
          sqlCreator.fillPara(stat);
          stat.executeQuery().next();
        } catch (SQLException e) {
          e.printStackTrace();
        }
        long et = System.currentTimeMillis();
        count.addAndGet(1);
        latency.addAndGet(et - pt);
      }
      try {
        jdbc.close();
        System.err.println("jdbc close succ");
      } catch (SQLException e) {
        // TODO Auto-generated catch block
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
  
  private void runPreCmdIfNeed(PreparedStatement stat, PrecompileSqlCreator creator) throws SQLException {
    if (creator.needRunPreCmd()) {
      String condition = conf.get(SqlConstants.PRE_CONDITION_KEY, SqlConstants.DEFAULT_PRE_CONDITION);
      String[] conditions = condition.split(";");
      for (String s : conditions) {
        // run tp condition first
        stat.execute(s);
      }
    }
  }
  
  private void test() throws SQLException {
    int port = conf.getInt(SqlConstants.PORT_KEY, SqlConstants.DEFAULT_PORT);
    String[] hosts = getHosts();
    
    DataBaseConnector jdbc = TestUtil.getDBConnector(hosts[0], port, conf);
    PreparedStatement stat = jdbc.prepareStatement(sqlCreator.sqlTemplate());
    stat.execute("set ngmr.exec.mode=local");
    sqlCreator.fillPara(stat);
    stat.executeQuery();
    jdbc.dumpResult(stat.getResultSet());
    jdbc.close();
  }
  
  private String[] getHosts() {
    String[] hosts = null;
    // choose tp hosts
    String tpHosts = conf.get(SqlConstants.TP_HOSTS_KEY);
    assert tpHosts != null : "must specify tp hosts";
    hosts = tpHosts.split(";");
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

    sqlCreator = TestUtil.getPrecompileSqlCreator(conf);
    if (sqlCreator == null) {
      return;
    }
    
    TestUtil.printConf(conf);
    TestUtil.initTestInput(conf);
    
    int threadnum = conf.getInt(Constants.THREAD_NUM_KEY, Constants.DEFAULT_THREAD_NUM);
    int interval = conf.getInt(Constants.INTERVAL_KEY, Constants.DEFALUT_INTERVAL) * 1000;
    int port = conf.getInt(SqlConstants.PORT_KEY, SqlConstants.DEFAULT_PORT);
    String[] hosts = getHosts();
    
    TestUtil.initRandom();
    boolean isTest = conf.getBoolean(Constants.RUN_IN_TEST_MODE_KEY, Constants.DEFAULT_RUN_IN_TEST_MODE);
    if (isTest) {
      test();
      return;
    }
    
    boolean fetchResult = conf.getBoolean(SqlConstants.FETCH_RESULT, false);
    for (int i=0; i<threadnum; ++i) {
      threads.add(new Worker(hosts[i % hosts.length], port, fetchResult));
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
  
  public static void main(String args[]) throws IOException, NumberFormatException, InterruptedException, SQLException {
    TestSQLUsePrecompile runner = new TestSQLUsePrecompile();
    runner.installSignalHander();
    runner.start();
    System.err.println("Finished");
  }
}
