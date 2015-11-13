package io.transwarp.sql.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;



public class DB2JDBCConnector implements DataBaseConnector {
  private static String driverName = "com.ibm.db2.jcc.DB2Driver";
  private Connection connection = null;
  private Statement statement = null;

  public DB2JDBCConnector(String url, String user, String passwd) {
    try {
      Class.forName(driverName);
    } catch (Exception e) {
      e.printStackTrace();
    }
    try {
      connection = DriverManager.getConnection(url, user, passwd);
    } catch (SQLException e) {
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
    ResultSetMetaData resultSetMD = rs.getMetaData();
    int columnLength = resultSetMD.getColumnCount();
    
    try {
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

  public static void run(String url, String user, String passwd) {
    DB2JDBCConnector conn = new DB2JDBCConnector(url, user, passwd);
    Statement stat = conn.getStatement();
    try {
      runCmd(conn, stat);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
  
  private static void runCmd(DB2JDBCConnector conn, Statement stat) throws SQLException {
    Scanner scanner = new Scanner(System.in);
    while (true) {
      String cmd = scanner.nextLine();
      stat.execute(cmd);
      conn.dumpResult(stat.getResultSet());
    }
  }

  public static void main(String[] args) {
    if (args.length != 3) {
      System.out.println("Usage : url user passwd");
      return;
    }
    run(args[0], args[1], args[1]);
  }
}
