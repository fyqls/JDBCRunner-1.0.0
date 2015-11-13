package io.transwarp.sql.jdbc;

import io.transwarp.constant.Constants;
import io.transwarp.sql.util.SqlConstants;

import java.io.File;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;



public class JDBCConnector implements DataBaseConnector {
  private static String driverName = null; //"org.apache.hive.jdbc.HiveDriver"; "org.apache.hadoop.hive.jdbc.HiveDriver";
  private Connection connection = null;
  private Statement statement = null;

  static {
    ArrayList<String> classpath = new ArrayList<String>();
    classpath.add("/etc/hadoop/conf");
    classpath.add("/etc/hive/conf");
    File hiveLib = new File("/usr/lib/hive/lib");
    File hadoopLib = new File("/usr/lib/hadoop");
    File[] libDirs = new File[] {hiveLib, hadoopLib};
    for (File lib : libDirs) {
      for (File jar : lib.listFiles()) {
        if (jar.getAbsolutePath().endsWith("jar")) {
          classpath.add(jar.getAbsolutePath());
        }
      }
    }
    addClassPath(classpath);
  }

  public JDBCConnector() {
  }
  public JDBCConnector(String host, int port) {
      this(host,port,new Configuration());
  }
  
  public JDBCConnector(String host, int port,Configuration conf) {
    try {
      String hiveServer = conf.get(SqlConstants.HIVE_SERVER,"hiveServer1");
      if(hiveServer.equals("hiveServer2")){
         driverName = "org.apache.hive.jdbc.HiveDriver";
      }else{
         driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
      }
      Class.forName(driverName);
      //String principal = conf.get(SqlConstants.PRINCIPAL,"localhost");
      if(hiveServer.equals("hiveServer2")){
        String principal = getHostNameForLiunx();
        if(principal.equals("UnknownHost")){
          throw new Exception("the hostName cannot find .....");
        }
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("hive/" + principal + "@TDH", conf.get(Constants.KEYTAB_LOCALTION,Constants.DEFAULT_KEYTAB_LOCALTION));

        System.out.println("jdbc:hive2://" + host + ":" + port + "/default;principal=hive/" + host + "@TDH");
        connection = DriverManager.getConnection("jdbc:hive2://"+host+":"+port+"/default;principal=hive/"+host+"@TDH", "", "");
      }else{
        connection = DriverManager.getConnection("jdbc:transwarp://" + host + ":" + port + "/default", "", "");
        System.out.println("jdbc:transwarp://" + host + ":" + port + "/default");
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  @Override
  public Connection getConnection() {
    return connection;
  }
  
  @Override
  public void close() throws SQLException {
    this.connection.close();
  }

  @Override
  public Statement getStatement() {
    if (statement == null) {
      try {
        statement = connection.createStatement();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    return statement;
  }
  
  @Override
  public PreparedStatement prepareStatement(String sql) {
    try {
      return connection.prepareStatement(sql);
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public void dumpResult(ResultSet rs) throws SQLException {
    StringBuffer sb = new StringBuffer();
    try {
      ResultSetMetaData resultSetMD = rs.getMetaData();
      int columnLength = resultSetMD.getColumnCount();
      System.out.println("rs.size:  " + columnLength);
      while (rs.next()) {
        for (int i = 1; i <= columnLength; i++) {
          // System.out.print(resultSetMD.getColumnClassName(i));
          sb.append(rs.getString(i));
          if (i != columnLength) {
            sb.append("\t");
          } else {
            sb.append("\n");
          }
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    System.out.println("result : \n" + sb.toString());
  }

  private static void addClassPath(ArrayList<String> path) {
    Method addURL = null;
    ClassLoader system = ClassLoader.getSystemClassLoader();
    try {
      addURL = URLClassLoader.class.getDeclaredMethod("addURL", new Class[] {URL.class});
      addURL.setAccessible(true);
      for (String p : path) {
        File f = new File(p);
        if (f.isFile()) {
          addURL.invoke(system, new Object[] {f.toURI().toURL()});
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void run(String host, String port) {
    JDBCConnector conn = new JDBCConnector(host, Integer.parseInt(port));
    Statement stat = conn.getStatement();
    try {
      runCmd(conn, stat);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
  
  private static void runCmd(JDBCConnector conn, Statement stat) throws SQLException {
    Scanner scanner = new Scanner(System.in);
    while (true) {
      String cmd = scanner.nextLine();
      stat.execute(cmd);
      conn.dumpResult(stat.getResultSet());
    }
  }

    public static String getHostNameForLiunx() {
        try {
            return (InetAddress.getLocalHost()).getHostName();
        } catch (UnknownHostException uhe) {
            String host = uhe.getMessage(); // host = "hostname: hostname"
            if (host != null) {
                int colon = host.indexOf(':');
                if (colon > 0) {
                    return host.substring(0, colon);
                }
            }
            return "UnknownHost";
        }
    }

//  public static void main(String[] args) {
//    if (args.length != 2) {
//      System.out.println("Usage : host port");
//      return;
//    }
//    run(args[0], args[1]);
//  }
  
}
