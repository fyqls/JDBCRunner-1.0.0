package io.transwarp.sql;

import io.transwarp.constant.Constants;
import io.transwarp.sql.jdbc.DataBaseConnector;
import io.transwarp.sql.util.SqlConstants;
import io.transwarp.sql.util.TestUtil;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * Test with jdbc
 * @author xiliu
 *
 */
public class TestSQLBatch {
  static {
    Configuration.addDefaultResource("hbase-test.xml");
  }
  
  private Configuration conf = HBaseConfiguration.create();
  private Counter counter;
  private SqlCreator sqlCreator = null;
  
  private ExecutorService pool = null;
  private BlockingQueue<ConnectorWarpper> queue = null;
  
  private class ConnectorWarpper {
    DataBaseConnector connector;
    long latency;
    
    public ConnectorWarpper(DataBaseConnector connector) {
      this.connector = connector;
    }
    
    public DataBaseConnector getConnector() {
      return connector;
    }

    public void setLatency(long latency) {
      this.latency = latency;
    }
    
    public void stop() {
      try {
        connector.close();
        System.err.println("jdbc connection close succ");
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }
  
  private void runSql(Statement stat, SqlCreator creator) {
    try {
      stat.execute(creator.createSql());
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
  
  class Worker extends Thread {
    ConnectorWarpper connector = null;
    CountDownLatch latch = null;
    
    public Worker(ConnectorWarpper connector, CountDownLatch latch) {
      this.connector = connector;
      this.latch = latch;
    }
    
    public void run() {
      Statement stat = connector.getConnector().getStatement();
      
      long pt = System.currentTimeMillis();
      runSql(stat, sqlCreator);
      try {
        stat.getResultSet().next();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      long et = System.currentTimeMillis();
      connector.setLatency(et - pt);
      try {
        queue.put(connector);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      latch.countDown();
    }
  }
  
  class Counter extends Thread {
    public boolean stopped;
    
    public Counter () {
      this.stopped = false;
    }
    
    public void run() {
      try {
        while (!stopped) {
          CountDownLatch latch = new CountDownLatch(queue.size());
          int size = queue.size();
          for (int i=0; i<size; ++i) {
            pool.submit(new Worker(queue.poll(), latch));
          }
          latch.await();
          
          long max = 0;
          long min = Long.MAX_VALUE;
          int total = 0;
          int count = 0;
          for (ConnectorWarpper warpper : queue) {
            max = (max > warpper.latency) ? max : warpper.latency;
            min = (min < warpper.latency) ? min : warpper.latency;
            total += warpper.latency;
            ++count;
          }
          System.out.println("count : " + count + ", max : " + max + ", min : " + min + ", avg : " + total/count);
        }
        for (ConnectorWarpper warpper : queue) {
          warpper.stop();
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
  
  private void test() throws SQLException {
    int port = conf.getInt(SqlConstants.PORT_KEY, SqlConstants.DEFAULT_PORT);
    String[] hosts = getHosts(sqlCreator);
    
    DataBaseConnector jdbc = TestUtil.getDBConnector(hosts[0], port, conf);
    Statement stat = jdbc.getStatement();
    runPreCmdIfNeed(stat, sqlCreator);
    runSql(stat, sqlCreator);
    jdbc.dumpResult(stat.getResultSet());
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
    counter.stopped = true;
  }
  
  public void installSignalHander() {
    SqlSignalHandler handler = new SqlSignalHandler();
    Signal.handle(new Signal("TERM"), handler);
    Signal.handle(new Signal("INT"), handler);
  }
  
  public void start() throws IOException, SQLException, InterruptedException {

    sqlCreator = TestUtil.getSqlCreator(conf);
    if (sqlCreator == null) {
      return;
    }
    
    TestUtil.printConf(conf);
    TestUtil.initTestInput(conf);
    
    int threadnum = conf.getInt(Constants.THREAD_NUM_KEY, Constants.DEFAULT_THREAD_NUM);
    int port = conf.getInt(SqlConstants.PORT_KEY, SqlConstants.DEFAULT_PORT);
    String[] hosts = getHosts(sqlCreator);
    
    TestUtil.initRandom();
    boolean isTest = conf.getBoolean(Constants.RUN_IN_TEST_MODE_KEY, Constants.DEFAULT_RUN_IN_TEST_MODE);
    if (isTest) {
      test();
      return;
    }
    
    this.pool = Executors.newFixedThreadPool(threadnum);
    queue = new ArrayBlockingQueue<ConnectorWarpper>(threadnum);
    for (int i=0; i<threadnum; ++i) {
      DataBaseConnector connector = TestUtil.getDBConnector(hosts[i % hosts.length], port, conf);
      System.err.println("jdbc connect succ");
      try {
        runPreCmdIfNeed(connector.getStatement(), sqlCreator);
      } catch (SQLException e) {
        e.printStackTrace();
        return;
      }
      queue.put(new ConnectorWarpper(connector));
    }
    
    counter = new Counter();
    counter.start();
    counter.join();
    pool.shutdownNow();
  }
  
  public static void main(String args[]) throws IOException, NumberFormatException, InterruptedException, SQLException {
    TestSQLBatch runner = new TestSQLBatch();
    runner.installSignalHander();
    runner.start();
    System.err.println("Finished");
  }
}
