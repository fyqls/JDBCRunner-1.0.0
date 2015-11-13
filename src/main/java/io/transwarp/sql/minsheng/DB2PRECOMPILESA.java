package io.transwarp.sql.minsheng;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import io.transwarp.sql.PrecompileSqlCreator;
import io.transwarp.sql.util.TestUtil;

/*
 * custno 0
 * fmanager 1
 * cmanager 2
 * orgid 3
 */
public abstract class DB2PRECOMPILESA implements PrecompileSqlCreator {
  
  public boolean needRunPreCmd() {
    return false;
  }
  
  //row key
 public static class Wangwangwang extends DB2PRECOMPILESA {
   static String template = "SELECT c_first_name FROM tpcds_hyperbase_2.customer WHERE c_customer_sk = ? limit 1";

   @Override
   public void fillPara(PreparedStatement stat) throws SQLException {
     stat.setInt(1, Integer.parseInt(TestUtil.randomParameter(0)));
   }

   @Override
   public String sqlTemplate() {
     return template;
   }
   
   @Override
   public boolean needRunPreCmd() {
     return true;
   }
 }
  
  // row key
  public static class CustBaseInfoQuery extends DB2PRECOMPILESA {
    static String template = "SELECT * FROM SA.FIN_CUST_INFO_BASE WHERE CUSTNO = ?";

    @Override
    public void fillPara(PreparedStatement stat) throws SQLException {
      stat.setString(1, TestUtil.randomParameter(0));
    }

    @Override
    public String sqlTemplate() {
      return template;
    }
  }
  
  // lookup join with global index
  public static class CustExtendInfoQuery extends DB2PRECOMPILESA {
    static String template = "SELECT BASE.CUSTNO," +
       "BASE.CUSTNAME," +
       "BASE.SEX," +
       "SUB.AGEPART," +
       "SUB.DEPOSITBALAVGL3MPART," +
       "CON.DEPOSITBALAVGL3M," +
       "CON.FASSETBAL," +
       "CMAN.CEMPID," +
       "ORG.ORGID" +
       " FROM SA.FIN_CUST_INFO_BASE BASE" +
                     " INNER JOIN" +
                        " SA.FIN_CUST_INFO_SUB SUB" +
                     " ON BASE.CUSTNO = SUB.CUSTNO" +
                  " INNER JOIN" +
                     " SA.FIN_CONFINPROFILE_BASE CON" +
                  " ON CON.CUSTNO = BASE.CUSTNO" +
               " INNER JOIN" +
                  " SA.FIN_CUST_CMANAGER CMAN" +
               " ON CMAN.CUSTNO = BASE.CUSTNO" +
      " INNER JOIN" +
         " SA.FIN_CUST_ORG ORG" +
      " ON ORG.CUSTNO = BASE.CUSTNO" +
  " WHERE BASE.CUSTNO = ?";

    @Override
    public void fillPara(PreparedStatement stat) throws SQLException {
      stat.setString(1, TestUtil.randomParameter(0));
    }

    @Override
    public String sqlTemplate() {
      return template;
    }
  }
  
  // global index
  public static class FManagerCustCltInfoQuery extends DB2PRECOMPILESA {
    static String template = "SELECT LEA.RECCREATEDATE," +
        "LEA.CLTID," +
        "LEA.CUSTNO," +
        "LEA.LEADOPPPHASE," +
        "LEA.USERID," +
        "LEA.ORGID," +
        "LEA.RECCREATEUID," +
        "CAM.EXECUTEDATE," +
        "FM.FEMPID," +
        "BASE.CUSTNAME," +
        "BASE.CUSTTYPE," +
        "BASE.SEX" +
        " FROM SA.FIN_CUST_FMANAGER FM" +
        " INNER JOIN" +
          " SA.FIN_LEADOPPS LEA" +
          " ON FM.CUSTNO = LEA.CUSTNO" +
        " INNER JOIN" +
          " SA.FIN_CUST_INFO_BASE BASE" +
          " ON BASE.CUSTNO = LEA.CUSTNO" +
        " INNER JOIN" +
          " SA.FIN_CAMPLEADTEM CAM" +
          " ON CAM.CLTID = LEA.CLTID" +
        " WHERE FM.FEMPID = ?";
    @Override
    public void fillPara(PreparedStatement stat) throws SQLException {
      stat.setString(1, TestUtil.randomParameter(1));
    }
    @Override
    public String sqlTemplate() {
      return template;
    }
  }
  
  // global index
  public static class OrgCustQuery extends DB2PRECOMPILESA {
    static String template = "SELECT SUB.FASSETBALPART AS FASSETBALPART," +
        "SUM (CON.FASSETBAL) / 10000 AS FASSETBAL," +
        "SUM (CON.FASSETBALMAX) / 10000 AS FASSETBALMAX," +
        "SUM (CON.FASSETBALAVGMMAX) / 10000 AS FASSETBALAVGMMAX" +
   " FROM       SA.FIN_CUST_ORG ORG" +
          " INNER JOIN" +
             " SA.FIN_CONFINPROFILE_BASE CON" +
          " ON ORG.CUSTNO = CON.CUSTNO" +
       " INNER JOIN" +
          " SA.FIN_CUST_INFO_SUB SUB" +
       " ON SUB.CUSTNO = CON.CUSTNO" +
   " WHERE ORG.ORGID = ? GROUP BY SUB.FASSETBALPART" +
        " ORDER BY FASSETBALPART";
    @Override
    public void fillPara(PreparedStatement stat) throws SQLException {
      stat.setString(1, TestUtil.randomParameter(2));
    }
    @Override
    public String sqlTemplate() {
      return template;
    }
  }
  
  public static class DemoQuery1 extends DB2PRECOMPILESA {
    static String template = "SELECT COUNT(B.CUSTNO) HZBS, " + 
       "SUM(B.ACTFLAG) HZJE " + 
  "FROM (SELECT CUSTNO FROM SA.FIN_CUST_ORG WHERE ORGID=? " +
          "UNION " +
        "SELECT CUSTNO FROM SA.FIN_CUST_FMANAGER WHERE FEMPID=? " +
          "UNION " +
        "SELECT CUSTNO FROM SA.FIN_CUST_CMANAGER WHERE CEMPID=?) AS GTEMP " +
    "INNER JOIN SA.FIN_CUST_INFO_BASE A " +
      "ON A.CUSTNO = GTEMP.CUSTNO " +
    "INNER JOIN SA.FIN_CUST_INFO_SUB B " +
      "ON B.CUSTNO = GTEMP.CUSTNO " +
  "WHERE 1=1 AND B.AGEPART >= 5 AND B.FASSETBALAVGMPART <= 10 AND B.VIPCARDLEVELPART >= 3 AND B.VIPLEVELPART <=7";

    @Override
    public void fillPara(PreparedStatement stat) throws SQLException {
      stat.setString(1, TestUtil.randomParameter(2));
      stat.setString(2, TestUtil.randomParameter(1));
      stat.setString(3, TestUtil.randomParameter(2));
    }

    @Override
    public String sqlTemplate() {
      return template;
    }
  }
  
  public static class DemoQuery2 extends DB2PRECOMPILESA {
    static String template = "SELECT SUBQUERY.CUSTTYPE, COUNT(*) " +
  "FROM (SELECT CASE WHEN CB.CUSTTYPE = 1 THEN 'NO' ELSE 'YES' END CUSTTYPE " +
          "FROM (SELECT CUSTNO FROM SA.FIN_CUST_CMANAGER WHERE CEMPID = ?) CGT " +
            "INNER JOIN SA.FIN_CUST_INFO_BASE CB " +
              "ON CGT.CUSTNO = CB.CUSTNO " +
            "INNER JOIN SA.FIN_CUST_INFO_SUB SB " +
              "ON CGT.CUSTNO = SB.CUSTNO " +
            "INNER JOIN SA.FIN_CONFINPROFILE_BASE PB " +
              "ON CGT.CUSTNO = PB.CUSTNO " +
            "INNER JOIN SA.FIN_CUST_ORG ORG " +
              "ON CGT.CUSTNO = ORG.CUSTNO) SUBQUERY " +
  "GROUP BY SUBQUERY.CUSTTYPE";

    @Override
    public void fillPara(PreparedStatement stat) throws SQLException {
      stat.setString(1, TestUtil.randomParameter(2));
    }

    @Override
    public String sqlTemplate() {
      return template;
    }
  }
}
