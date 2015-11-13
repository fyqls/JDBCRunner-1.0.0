package io.transwarp.sql.minsheng;

import io.transwarp.sql.SqlCreator;
import io.transwarp.sql.util.TestUtil;

/*
 * custno 0
 * fmanager 1
 * cmanager 2
 * orgid 3
 */
public abstract class DB2SADATASQLS implements SqlCreator {
  @Override
  public boolean isTpSql() {
    return true;
  }

  @Override
  public boolean needRunPreCmd() {
    return false;
  }
  
  // row key
  public static class CustBaseInfoQuery extends DB2SADATASQLS {
    static String template = "SELECT * FROM SA.FIN_CUST_INFO_BASE WHERE CUSTNO = '%s'";

    @Override
    public String createSql() {
      String custno = TestUtil.randomParameter(0);
      return String.format(template, custno);
    }
  }
  
  // lookup join with global index
  public static class CustExtendInfoQuery extends DB2SADATASQLS {
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
  " WHERE BASE.CUSTNO = '%s'";
    
    @Override
    public String createSql() {
      String key = TestUtil.randomParameter(0);
      return String.format(template, key);
    }
  }
  
  // global index
  public static class FManagerCustCltInfoQuery extends DB2SADATASQLS {
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
        " WHERE FM.FEMPID = '%s'";
    @Override
    public String createSql() {
      String key = TestUtil.randomParameter(1);
      return String.format(template, key);
    }
  }
  
  // global index
  public static class OrgCustQuery extends DB2SADATASQLS {
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
   " WHERE ORG.ORGID = '%s' GROUP BY SUB.FASSETBALPART" +
        " ORDER BY FASSETBALPART";
    @Override
    public String createSql() {
      String key = TestUtil.randomParameter(2);
      return String.format(template, key);
    }
  }
  
  public static class DemoQuery1 extends DB2SADATASQLS {
    static String template = "SELECT COUNT(B.CUSTNO) HZBS, " + 
       "SUM(B.ACTFLAG) HZJE " + 
  "FROM (SELECT CUSTNO FROM SA.FIN_CUST_ORG WHERE ORGID='%s' " +
          "UNION " +
        "SELECT CUSTNO FROM SA.FIN_CUST_FMANAGER WHERE FEMPID='%s' " +
          "UNION " +
        "SELECT CUSTNO FROM SA.FIN_CUST_CMANAGER WHERE CEMPID='%s') AS GTEMP " +
    "INNER JOIN SA.FIN_CUST_INFO_BASE A " +
      "ON A.CUSTNO = GTEMP.CUSTNO " +
    "INNER JOIN SA.FIN_CUST_INFO_SUB B " +
      "ON B.CUSTNO = GTEMP.CUSTNO " +
  "WHERE 1=1 AND B.AGEPART >= 5 AND B.FASSETBALAVGMPART <= 10 AND B.VIPCARDLEVELPART >= 3 AND B.VIPLEVELPART <=7";
    
    @Override
    public String createSql() {
      return String.format(template, TestUtil.randomParameter(2), TestUtil.randomParameter(1), TestUtil.randomParameter(2));
    }
  }
  
  public static class DemoQuery2 extends DB2SADATASQLS {
    static String template = "SELECT SUBQUERY.CUSTTYPE, COUNT(*) " +
  "FROM (SELECT CASE WHEN CB.CUSTTYPE = 1 THEN 'NO' ELSE 'YES' END CUSTTYPE " +
          "FROM (SELECT CUSTNO FROM SA.FIN_CUST_CMANAGER WHERE CEMPID = '%s') CGT " +
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
    public String createSql() {
      return String.format(template,TestUtil.randomParameter(2));
    }
  }
  
  public static class InceptorDemo1 extends DB2SADATASQLS {
    static String template = "SELECT " +
  "SUB.VIPLEVELPART, " +
  "SUB.AGEPART, " +
  "COUNT(*) AS COUNT " +
"FROM SA.FIN_CUST_INFO_SUB SUB " +
"WHERE SUB.MAXASSETBALSEG >= '1' AND SUB.FASSETBALPART >= '1' " +
"GROUP BY SUB.VIPLEVELPART, SUB.AGEPART " +
"ORDER BY COUNT DESC fetch first 10 row only";
    
    @Override
    public String createSql() {
      return template;
    }
  }
  
  public static class InceptorDemo2 extends DB2SADATASQLS {
    static String template = "SELECT " +
  "ORG.ORGID, " +
  "COUNT(*) AS COUNT " +
"FROM SA.FIN_CUST_ORG ORG " +
"GROUP BY ORG.ORGID " +
"ORDER BY COUNT DESC fetch first 10 row only";
    
    @Override
    public String createSql() {
      return template;
    }
  }
  
  public static class InceptorDemo3 extends DB2SADATASQLS {
    static String template = "SELECT " +
  "ORG.ORGID, " +
  "COUNT(*) AS COUNT " +
"FROM SA.FIN_CUST_INFO_BASE BASE " +
  "INNER JOIN SA.FIN_CUST_ORG ORG " +
"ON BASE.CUSTNO=ORG.CUSTNO " +
"WHERE BASE.CUSTAGE > 60 " +
"GROUP BY ORG.ORGID " +
"ORDER BY COUNT DESC " +
"fetch first 10 row only";
    
    @Override
    public String createSql() {
      return template;
    }
  }
  
  public static class InceptorDemo4 extends DB2SADATASQLS {
    static String template = "SELECT " +
  "ORG.ORGID, " +
  "SUM(CONF.FASSETBALAVGL3M) AS SUM " +
"FROM SA.FIN_CUST_INFO_BASE BASE " +
  "INNER JOIN SA.FIN_CUST_ORG ORG " +
"ON BASE.CUSTNO=ORG.CUSTNO " +
  "INNER JOIN SA.FIN_CONFINPROFILE_BASE CONF " +
"ON CONF.CUSTNO=ORG.CUSTNO " +
"WHERE BASE.CUSTAGE>60 " +
"GROUP BY ORG.ORGID " +
"ORDER BY SUM DESC fetch first 10 row only";
    
    @Override
    public String createSql() {
      return template;
    }
  }
  
  public static class InceptorDemo5 extends DB2SADATASQLS {
    static String template = "SELECT TX_CD, AVG(USD_AMT) FROM SA.M_NIN_AML_V_MDS_TRANS WHERE TX_DT < '20141205' AND TX_DT > '20141105' GROUP BY TX_CD";
    
    @Override
    public String createSql() {
      return template;
    }
  }
  
  public static class InceptorDemo6 extends DB2SADATASQLS {
    static String template = "SELECT TX_CD, MAX(AMT), MIN(AMT) FROM SA.M_NIN_AML_V_MDS_TRANS WHERE TX_DT < '20141205' AND TX_DT > '20141105' GROUP BY TX_CD";
    
    @Override
    public String createSql() {
      return template;
    }
  }
}
