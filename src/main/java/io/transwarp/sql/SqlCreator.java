package io.transwarp.sql;

public interface SqlCreator {
  public String createSql();
  
  public boolean isTpSql();
  
  public boolean needRunPreCmd();
}
