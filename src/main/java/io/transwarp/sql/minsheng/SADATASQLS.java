package io.transwarp.sql.minsheng;

import io.transwarp.sql.SqlCreator;
import io.transwarp.sql.util.TestUtil;


/*
 * custno 0
 * fmanager 1
 * cmanager 2
 * orgid 3
 */
public abstract class SADATASQLS implements SqlCreator {
  @Override
  public boolean isTpSql() {
    return true;
  }

  @Override
  public boolean needRunPreCmd() {
    return true;
  }
  
  // row key
  public static class CustBaseInfoQuery extends SADATASQLS {
    static String template = "SELECT * FROM HYPERBASE_FIN_CUST_INFO_BASE WHERE CUSTNO = '%s'";

    @Override
    public String createSql() {
      String custno = TestUtil.randomParameter(0);
      return String.format(template, custno);
    }
  }
  
  // lookup join with global index
  public static class CustExtendInfoQuery extends SADATASQLS {
    static String template = "SELECT BASE.CUSTNO," +
       "BASE.CUSTNAME," +
       "BASE.SEX," +
       "SUB.AGEPART," +
       "SUB.DEPOSITBALAVGL3MPART," +
       "CON.DEPOSITBALAVGL3M," +
       "CON.FASSETBAL," +
       "CMAN.CEMPID," +
       "ORG.ORGID" +
       " FROM HYPERBASE_FIN_CUST_INFO_BASE BASE" +
                     " INNER JOIN" +
                        " HYPERBASE_FIN_CUST_INFO_SUB SUB" +
                     " ON BASE.CUSTNO = SUB.CUSTNO" +
                  " INNER JOIN" +
                     " HYPERBASE_FIN_CONFINPROFILE_BASE CON" +
                  " ON CON.CUSTNO = BASE.CUSTNO" +
               " INNER JOIN" +
                  " HYPERBASE_FIN_CUST_CMANAGER CMAN" +
               " ON CMAN.CUSTNO = BASE.CUSTNO" +
      " INNER JOIN" +
         " HYPERBASE_FIN_CUST_ORG ORG" +
      " ON ORG.CUSTNO = BASE.CUSTNO" +
  " WHERE BASE.CUSTNO = '%s'";
    
    @Override
    public String createSql() {
      String key = TestUtil.randomParameter(0);
      return String.format(template, key);
    }
  }
  
  // global index
  public static class FManagerCustCltInfoQuery extends SADATASQLS {
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
        " FROM HYPERBASE_FIN_CUST_FMANAGER FM" +
        " INNER JOIN" +
          " HYPERBASE_FIN_LEADOPPS LEA" +
          " ON FM.CUSTNO = LEA.CUSTNO" +
        " INNER JOIN" +
          " HYPERBASE_FIN_CUST_INFO_BASE BASE" +
          " ON BASE.CUSTNO = LEA.CUSTNO" +
        " INNER JOIN" +
          " HYPERBASE_FIN_CAMPLEADTEM CAM" +
          " ON CAM.CLTID = LEA.CLTID" +
        " WHERE FM.FEMPID = '%s'";
    @Override
    public String createSql() {
      String key = TestUtil.randomParameter(1);
      return String.format(template, key);
    }
  }
  
  // global index
  public static class OrgCustQuery extends SADATASQLS {
    static String template = "SELECT SUB.FASSETBALPART AS FASSETBALPART," +
        "SUM (CON.FASSETBAL) / 10000 AS FASSETBAL," +
        "SUM (CON.FASSETBALMAX) / 10000 AS FASSETBALMAX," +
        "SUM (CON.FASSETBALAVGMMAX) / 10000 AS FASSETBALAVGMMAX" +
   " FROM       HYPERBASE_FIN_CUST_ORG ORG" +
          " INNER JOIN" +
             " HYPERBASE_FIN_CONFINPROFILE_BASE CON" +
          " ON ORG.CUSTNO = CON.CUSTNO" +
       " INNER JOIN" +
          " HYPERBASE_FIN_CUST_INFO_SUB SUB" +
       " ON SUB.CUSTNO = CON.CUSTNO" +
   " WHERE ORG.ORGID = '%s' GROUP BY SUB.FASSETBALPART" +
        " ORDER BY FASSETBALPART";
    @Override
    public String createSql() {
      String key = TestUtil.randomParameter(3);
      return String.format(template, key);
    }
  }
  
  public static class DemoQuery1 extends SADATASQLS {
    static String template = "SELECT COUNT(B.CUSTNO) HZBS, " + 
       "SUM(B.ACTFLAG) HZJE " + 
  "FROM (SELECT CUSTNO FROM HYPERBASE_FIN_CUST_ORG WHERE ORGID='%s' " +
          "UNION " +
        "SELECT CUSTNO FROM HYPERBASE_FIN_CUST_FMANAGER WHERE FEMPID='%s' " +
          "UNION " +
        "SELECT CUSTNO FROM HYPERBASE_FIN_CUST_CMANAGER WHERE CEMPID='%s') AS GTEMP " +
    "INNER JOIN HYPERBASE_FIN_CUST_INFO_BASE A " +
      "ON A.CUSTNO = GTEMP.CUSTNO " +
    "INNER JOIN HYPERBASE_FIN_CUST_INFO_SUB B " +
      "ON B.CUSTNO = GTEMP.CUSTNO " +
  "WHERE 1=1 AND B.AGEPART >= 5 AND B.FASSETBALAVGMPART <= 10 AND B.VIPCARDLEVELPART >= 3 AND B.VIPLEVELPART <=7";
    
    @Override
    public String createSql() {
      return String.format(template, TestUtil.randomParameter(3), TestUtil.randomParameter(1), TestUtil.randomParameter(2));
    }
  }
  
  public static class DemoQuery2 extends SADATASQLS {
    static String template = "SELECT SUBQUERY.CUSTTYPE, COUNT(*) " +
  "FROM (SELECT CASE WHEN CB.CUSTTYPE = 1 THEN 'NO' ELSE 'YES' END CUSTTYPE " +
          "FROM (SELECT CUSTNO FROM HYPERBASE_FIN_CUST_CMANAGER WHERE CEMPID = '%s') CGT " +
            "INNER JOIN HYPERBASE_FIN_CUST_INFO_BASE CB " +
              "ON CGT.CUSTNO = CB.CUSTNO " +
            "INNER JOIN HYPERBASE_FIN_CUST_INFO_SUB SB " +
              "ON CGT.CUSTNO = SB.CUSTNO " +
            "INNER JOIN HYPERBASE_FIN_CONFINPROFILE_BASE PB " +
              "ON CGT.CUSTNO = PB.CUSTNO " +
            "INNER JOIN HYPERBASE_FIN_CUST_ORG ORG " +
              "ON CGT.CUSTNO = ORG.CUSTNO) SUBQUERY " +
  "GROUP BY SUBQUERY.CUSTTYPE";
    
    @Override
    public String createSql() {
      return String.format(template,TestUtil.randomParameter(2));
    }
  }
  
  // update
  public static class UpdateConfinprofileBase extends SADATASQLS {
    static String template = "UPDATE HYPERBASE_FIN_CONFINPROFILE_BASE SET FASSETBAL='%s' WHERE CUSTNO='%s'";

    @Override
    public String createSql() {
      String key = TestUtil.randomParameter(0);
      String value = "12345";
      return String.format(template, value, key);
    }
  }
  
  // delete
  public static class DeleteCustInfoSub extends SADATASQLS {
    static String template = "DELETE FROM hyperbase_fin_cust_org WHERE custno='%s'";
    
    @Override
    public String createSql() {
      return String.format(template, TestUtil.randomParameter(0));
    }
  }
  
  
  public static class NewDemo1 extends SADATASQLS {
    public String template = "SELECT COUNT(*) FROM " +
"(" +
 "SELECT A.CUSTNO CUSTNO,A.CUSTNAME CUSTNAME,B.SIGNACCNO,B.NAME PNAME,B.COST/10000 COST,B.CURRENCY CURRENCY," +
 "B.OPDT OPDT,B.MADT MADT,CASE B.ISDEPOSIT WHEN 'B' THEN 'yes' ELSE 'no' END ISDEPOSIT,B.CHANNELID," +
 "M.CEMPID,F.FEMPID,S.SEMPID,C.ZIP,VIP.VIPLEVELPART VIPLEVELPART, C.ADDRESS,C.PHONE PHONE,C.MOBILEPHONE MOBILEPHONE, B.SIGNACCORG,B.ORGID,NVL(B.EARNINGRATECUR,0) AS EARNINGRATECUR " + 
 "FROM " +
 "( " +
  "SELECT " + 
   "A.CUSTNO CUSTNO,A.PRODUCT_ID PRODUCTID,B.PRODUCT_NAME NAME,C.rk.CARD_NUM SIGNACCNO,A.TRANS_AMT COST, " +
   "A.CURR_CD CURRENCY,B.PRODUCT_ATTR ISDEPOSIT,A.TRANS_CHANNEL CHANNELID,A.TRANS_ORG_NUM SIGNACCORG, " +
   "B.INCOME_RATE EARNINGRATECUR,B.PRODUCT_ESTAB_DATE OPDT,GX.ORGID, B.MATURE_DT MADT " +
  "FROM (SELECT CUSTNO FROM HYPERBASE_FIN_CUST_CMANAGER WHERE CEMPID='%s' UNION SELECT CUSTNO FROM HYPERBASE_FIN_CUST_SDELEGATE WHERE SEMPID='%s' ) AS GTEMP " +
  "INNER JOIN HYPERBASE_FIN_TSS_SELL A ON GTEMP.CUSTNO=A.CUSTNO " +
  "INNER JOIN HYPERBASE_FIN_TSS_PRODUCTINFO B ON B.PRODUCT_ID = A.PRODUCT_ID " +
  "INNER JOIN HYPERBASE_FIN_TSS_SIGN C ON C.ACCT_NUM = A.ACCT_NUM " +
  "LEFT JOIN HYPERBASE_FIN_CUST_ORG GX ON GX.CUSTNO = A.CUSTNO   " +
  "WHERE  1=1  AND (A.END_DT IS NULL)  AND B.MATURE_DT >= DATE('2013-04-01')  AND B.MATURE_DT <=  DATE('2013-04-30') " +     
 ") AS B " + 
 "INNER JOIN HYPERBASE_FIN_CUST_INFO_BASE A ON A.CUSTNO = B.CUSTNO " + 
 "INNER JOIN HYPERBASE_FIN_CUST_INFO_SUB VIP ON B.CUSTNO = VIP.CUSTNO " +
 "INNER JOIN HYPERBASE_FIN_CUST_CON_BASE C ON C.CUSTNO = B.CUSTNO  " +
 "LEFT JOIN HYPERBASE_FIN_CUST_CMANAGER M ON M.CUSTNO = B.CUSTNO  " +
 "LEFT JOIN HYPERBASE_FIN_CUST_FMANAGER F ON F.CUSTNO = B.CUSTNO " +
 "LEFT JOIN HYPERBASE_FIN_CUST_SDELEGATE S ON S.CUSTNO = B.CUSTNO " +
 "WHERE 1=1 " +
") PAGETALBE";

    @Override
    public String createSql() {
      return String.format(template, TestUtil.randomParameter(1), TestUtil.randomParameter(1));
    }
    
  }
  
  public static class NewDemo2 extends SADATASQLS {
    public String template = "explain SELECT A.rk.TRANSERNO,SPEJOUR.TRANSERNO AS SPETRANSERNO,B.CUSTNO,B.CUSTNAME,A.CZTRANSERNO,A.ACCDATE, " +
       "CASE WHEN A.CUSTNO <> D.UCUSTNO THEN 0 ELSE  INT(A.AMOUNT) END AS AMOUNT, " +
       "CASE WHEN A.CUSTNO <> '%s' THEN 0 ELSE  INT(A.PREBAL) END AS PREBAL, " +
       "CASE WHEN A.CUSTNO <> '%s' THEN 0 ELSE  INT(A.BALANCE) END AS BALANCE, " +
       "CASE WHEN A.CZTRANSFLAG = '1' THEN C.NAME ELSE C.NAME END NAME,A.CZTRANSFLAG,A.USID,D.CATCHSTAT , " +
       "CASE WHEN A.CUSTNO <> D.UCUSTNO THEN A.AMOUNT ELSE '' END  AUTHAMOUNT, " +
       "CASE WHEN A.CUSTNO <> D.UCUSTNO THEN E.CUSTNO ELSE '' END  AUTHCUSTNO, " +
       "CASE WHEN A.CUSTNO <> D.UCUSTNO THEN E.CUSTNAME  ELSE '' END   AUTHCUSTNAME " +
"FROM (SELECT * FROM HYPERBASE_JFX_TRANSJOUR_V T1 WHERE T1.CUSTNO = '%s' UNION SELECT * FROM HYPERBASE_JFX_TRANSJOUR_V T2 WHERE T2.UCUSTNO = '%s') A " +
  "LEFT JOIN HYPERBASE_FIN_CUST_INFO_BASE B ON (A.UCUSTNO = B.CUSTNO) " + 
  "LEFT JOIN HYPERBASE_JFX_CALCITEM C ON (A.CITEMID = C.CITEMID) " +
  "LEFT JOIN HYPERBASE_JFX_CONSUMEJOUR D ON A.rk.TRANSERNO = D.TRANSERNO " + 
  "LEFT JOIN HYPERBASE_JFX_SPETRANSJOUR SPEJOUR ON  A.rk.TRANSERNO=SPEJOUR.TRANSERNO " +
  "LEFT JOIN HYPERBASE_FIN_CUST_INFO_BASE E ON E.CUSTNO =  A.CUSTNO " +
"WHERE A.ACCDATE >= '2013-01-01-00.00.00.00000'";

    @Override
    public String createSql() {
      String custno = TestUtil.randomParameter(0);
      return String.format(template, custno, custno, custno, custno);
    }
    
  }
  
  public static class InceptorDemo1 extends SADATASQLS {
    static String template = "SELECT " +
  "SUB.VIPLEVELPART, " +
  "SUB.AGEPART, " +
  "COUNT(*) AS COUNT " +
"FROM MEM_FIN_CUST_INFO_SUB SUB " +
"WHERE SUB.MAXASSETBALSEG >= '1' AND SUB.FASSETBALPART >= '1' " +
"GROUP BY SUB.VIPLEVELPART, SUB.AGEPART " +
"ORDER BY COUNT DESC LIMIT 10";
    
    @Override
    public boolean isTpSql() {
      return false;
    }
    
    @Override
    public String createSql() {
      return template;
    }
  }
  
  public static class InceptorDemo2 extends SADATASQLS {
    static String template = "SELECT " +
  "ORG.ORGID, " +
  "COUNT(*) AS COUNT " +
"FROM MEM_FIN_CUST_ORG ORG " +
"GROUP BY ORG.ORGID " +
"ORDER BY COUNT DESC LIMIT 10";
    
    @Override
    public boolean isTpSql() {
      return false;
    }
    
    @Override
    public String createSql() {
      return template;
    }
  }
  
  public static class InceptorDemo3 extends SADATASQLS {
    static String template = "SELECT " +
  "ORG.ORGID, " +
  "COUNT(*) AS COUNT " +
"FROM MEM_FIN_CUST_INFO_BASE BASE " +
  "INNER JOIN MEM_FIN_CUST_ORG ORG " +
"ON BASE.CUSTNO=ORG.CUSTNO " +
"WHERE BASE.CUSTAGE > 60 " +
"GROUP BY ORG.ORGID " +
"ORDER BY COUNT DESC " +
"LIMIT 10";
    
    @Override
    public boolean isTpSql() {
      return false;
    }
    
    @Override
    public String createSql() {
      return template;
    }
  }
  
  public static class InceptorDemo4 extends SADATASQLS {
    static String template = "SELECT " +
  "ORG.ORGID, " +
  "SUM(CONF.FASSETBALAVGL3M) AS SUM " +
"FROM MEM_FIN_CUST_INFO_BASE BASE " +
  "INNER JOIN MEM_FIN_CUST_ORG ORG " +
"ON BASE.CUSTNO=ORG.CUSTNO " +
  "INNER JOIN MEM_FIN_CONFINPROFILE_BASE CONF " +
"ON CONF.CUSTNO=ORG.CUSTNO " +
"WHERE BASE.CUSTAGE>60 " +
"GROUP BY ORG.ORGID " +
"ORDER BY SUM DESC LIMIT 10";
    
    @Override
    public boolean isTpSql() {
      return false;
    }
    
    @Override
    public String createSql() {
      return template;
    }
  }
  
  public static class InceptorDemo5 extends SADATASQLS {
    static String template = "SELECT TX_CD, AVG(USD_AMT) FROM MEM_M_NIN_AML_V_MDS_TRANS WHERE TX_DT < '20141205' AND TX_DT > '20141105' GROUP BY TX_CD";
    
    @Override
    public boolean isTpSql() {
      return false;
    }
    
    @Override
    public String createSql() {
      return template;
    }
  }
  
  public static class InceptorDemo6 extends SADATASQLS {
    static String template = "SELECT TX_CD, MAX(AMT), MIN(AMT) FROM MEM_M_NIN_AML_V_MDS_TRANS WHERE TX_DT < '20141205' AND TX_DT > '20141105' GROUP BY TX_CD";
    
    @Override
    public boolean isTpSql() {
      return false;
    }
    
    @Override
    public String createSql() {
      return template;
    }
  }
}
