package io.transwarp.sql.minsheng;

import io.transwarp.sql.SqlCreator;

public class Demo implements SqlCreator {
  private final String template = "select * " +
       "from hyperbase_sm_time_series x " +
         "inner join hyperbase_sm_meterasset m " +
       "on x.rowKey.id = m.ams_id " +
         "inner join hyperbase_sm_measure_point p " +
       "on p.rowkey = m.measure_point_id " +
         "inner join hyperbase_sm_public_transformer t " +
       "on t.rowkey = p.psr_id " +
       "where x.rowkey.id = '257960'";
  
  @Override
  public String createSql() {
    return template;
  }

  @Override
  public boolean isTpSql() {
    return true;
  }

  @Override
  public boolean needRunPreCmd() {
    return true;
  }

}
