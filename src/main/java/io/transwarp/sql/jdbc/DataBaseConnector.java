package io.transwarp.sql.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public interface DataBaseConnector {
  public Connection getConnection();
  
  public Statement getStatement();
  
  public void close() throws SQLException;
  
  public void dumpResult(ResultSet rs) throws SQLException;
  
  public PreparedStatement prepareStatement(String sql);
}
