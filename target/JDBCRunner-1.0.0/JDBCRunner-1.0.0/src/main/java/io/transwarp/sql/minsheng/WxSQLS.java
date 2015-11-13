package io.transwarp.sql.minsheng;

import java.math.BigInteger;

import org.apache.hadoop.hbase.util.Pair;

import io.transwarp.sql.SqlCreator;
import io.transwarp.sql.util.TestUtil;

public abstract class WxSQLS {

  public static class QueryByHt implements SqlCreator {
    static String template = "select * from chengdao_hbase where dt <= '%s'";

    @Override
    public String createSql() {
      String ht = "9223370647735474807";
      return String.format(template,ht);
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
 
  public static class QueryByHphm implements SqlCreator {
    static String template = "select * from chengdao_hbase where hphm = '%s'";
    
    @Override
    public String createSql() {
      String hphm = "鲁D528E8";
      return String.format(template, hphm);
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
  
  public static class QueryByKkbh implements SqlCreator {
    static String template = "select * from chengdao_hbase where  kkbh = '%s'";
    
    @Override
    public String createSql() {
      String kkbh = "370400100006";
      return String.format(template, kkbh);
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
  
  // mem table test
  public static class QueryByHtHphm implements SqlCreator {
    static String template = "select * from chengdao_hbase where dt <= '%s' and  hphm = '%s'";

    @Override
    public String createSql() {
      String dt = "9223370647735474807";
      String hphm = "鲁D528E8";
      return String.format(template,dt,hphm);
    }

    @Override
    public boolean isTpSql() {
      return false;
    }

    @Override
    public boolean needRunPreCmd() {
      return false;
    }
  }
  
  // local index
  public static class QueryByHtKkbh implements SqlCreator {
    static String template = "select * from chengdao_hbase where dt <= '%s'  and kkbh = '%s' ";
    
    @Override
    public String createSql() {
      String dt= "9223370647735474807";
      String kkbh = "370400100006";
      return String.format(template, dt,kkbh);
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
  
  // mem table test
  public static class QueryByAll implements SqlCreator {
	  
	  static String template = "select * from orders limit 2";
    @Override
    public String createSql() {
      return String.format(template);
    }

    @Override
    public boolean isTpSql() {
      return true;
    }

    @Override
    public boolean needRunPreCmd() {
      return false;
    }
    
  }
}
