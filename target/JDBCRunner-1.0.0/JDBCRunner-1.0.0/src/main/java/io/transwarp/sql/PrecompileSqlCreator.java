package io.transwarp.sql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface PrecompileSqlCreator {
  public void fillPara(PreparedStatement stat) throws SQLException;
  
  public String sqlTemplate();
  
  public boolean needRunPreCmd();
}
