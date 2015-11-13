package io.transwarp.sql.hengfeng;

import io.transwarp.sql.SqlCreator;
import io.transwarp.sql.util.TestUtil;

/**
 * Created by gordonsong on 3/6/15.
 */
public abstract class INCEPTOR  implements SqlCreator{
    @Override
    public boolean isTpSql() {
        return true;
    }

    @Override
    public boolean needRunPreCmd() {
        return false;
    }
    

    public static class TMPINCEP0 extends INCEPTOR {

        static String template = "select * from orders limit 2";
        @Override
        public String createSql() {
            return String.format(template);
        }
    }

    public static class INCEP0 extends INCEPTOR {

        static String template = "select custno, count(*) as cnt from dcev_kl_tran_perf_ssd_1 group by custno order by cnt desc limit 50";
        @Override
        public String createSql() {
            return String.format(template);
        }
    }

    public static class INCEP1 extends INCEPTOR {

        static String template = "SELECT \n" +
                "  brchno_111,\n" +
                "  termcd_111,\n" +
                "  depoam_111,\n" +
                "  deponm_111,\n" +
                "  ROUND(\n" +
                "    cvstrt_111 / DECODE(depoam_111, 0, 99999999, depoam_111),\n" +
                "    8\n" +
                "  ) AS avgrat,\n" +
                "  bxdepo_111,\n" +
                "  bxdpnm_111,\n" +
                "  ROUND(\n" +
                "    bxcvst_111 / DECODE(bxdepo_111, 0, 99999999, bxdepo_111),\n" +
                "    8\n" +
                "  ) AS bxavgt,\n" +
                "  cxdepo_111,\n" +
                "  cxdpnm_111,\n" +
                "  ROUND(\n" +
                "    cxcvst_111 / DECODE(cxdepo_111, 0, 99999999, cxdepo_111),\n" +
                "    8\n" +
                "  ) AS avcxgt,\n" +
                "  cpdepo_111,\n" +
                "  cpdpnm_111,\n" +
                "  ROUND(\n" +
                "    cpcvst_111 / DECODE(cpdepo_111, 0, 99999999, cpdepo_111),\n" +
                "    8\n" +
                "  ) AS cpavgt,\n" +
                "  pudepo_111,\n" +
                "  pudpnm_111,\n" +
                "  ROUND(\n" +
                "    pucvst_111 / DECODE(pudepo_111, 0, 99999999, pudepo_111),\n" +
                "    8\n" +
                "  ) AS puavgt \n" +
                "FROM\n" +
                "  (SELECT \n" +
                "    regexp_REPLACE (\n" +
                "      regexp_REPLACE (c.brchna, '恒丰', ''),\n" +
                "      '分行',\n" +
                "      ''\n" +
                "    ) AS brchno_111,\n" +
                "    CASE\n" +
                "      WHEN months_between (a.matudt, a.opendt) <= 6 \n" +
                "      THEN '1 六个月以内(含)' \n" +
                "      WHEN months_between (a.matudt, a.opendt) > 6 \n" +
                "      AND months_between (a.matudt, a.opendt) <= 12 \n" +
                "      THEN '2 六个月至一年(含)' \n" +
                "      WHEN months_between (a.matudt, a.opendt) > 12 \n" +
                "      AND months_between (a.matudt, a.opendt) <= 36 \n" +
                "      THEN '3 一年至三年(含)' \n" +
                "      WHEN months_between (a.matudt, a.opendt) > 36 \n" +
                "      AND months_between (a.matudt, a.opendt) <= 60 \n" +
                "      THEN '4 三年至五年(含)' \n" +
                "      WHEN months_between (a.matudt, a.opendt) > 60 \n" +
                "      THEN '5 五年以上' \n" +
                "      ELSE '9 其他' \n" +
                "    END AS termcd_111,\n" +
                "    SUM(a.lastbl * b.cvrate) AS depoam_111,\n" +
                "    COUNT(1) AS deponm_111,\n" +
                "    SUM(\n" +
                "      a.lastbl * b.cvrate * nvl (a.instrt, 0)\n" +
                "    ) AS cvstrt_111,\n" +
                "    SUM(\n" +
                "      CASE\n" +
                "        WHEN SUBSTR(a.itemcd, 1, 6) = '246003' \n" +
                "        THEN a.lastbl * b.cvrate \n" +
                "        ELSE 0 \n" +
                "      END\n" +
                "    ) AS bxdepo_111,\n" +
                "    SUM(\n" +
                "      CASE\n" +
                "        WHEN SUBSTR(a.itemcd, 1, 6) = '246003' \n" +
                "        THEN 1 \n" +
                "        ELSE 0 \n" +
                "      END\n" +
                "    ) AS bxdpnm_111,\n" +
                "    SUM(\n" +
                "      CASE\n" +
                "        WHEN SUBSTR(a.itemcd, 1, 6) = '246003' \n" +
                "        THEN a.lastbl * b.cvrate * nvl (a.instrt, 0) \n" +
                "        ELSE 0 \n" +
                "      END\n" +
                "    ) AS bxcvst_111,\n" +
                "    SUM(\n" +
                "      CASE\n" +
                "        WHEN (\n" +
                "          SUBSTR(a.itemcd, 1, 4) IN (\n" +
                "            '2320',\n" +
                "            '2330',\n" +
                "            '2340',\n" +
                "            '2350',\n" +
                "            '2360',\n" +
                "            '2370',\n" +
                "            '2015',\n" +
                "            '2310'\n" +
                "          ) \n" +
                "          OR SUBSTR(a.itemcd, 1, 6) IN ('217002', '262010', '266001')\n" +
                "        ) \n" +
                "        THEN a.lastbl * b.cvrate \n" +
                "        ELSE 0 \n" +
                "      END\n" +
                "    ) AS cxdepo_111,\n" +
                "    SUM(\n" +
                "      CASE\n" +
                "        WHEN (\n" +
                "          SUBSTR(a.itemcd, 1, 4) IN (\n" +
                "            '2320',\n" +
                "            '2330',\n" +
                "            '2340',\n" +
                "            '2350',\n" +
                "            '2360',\n" +
                "            '2370',\n" +
                "            '2015',\n" +
                "            '2310'\n" +
                "          ) \n" +
                "          OR SUBSTR(a.itemcd, 1, 6) IN ('217002', '262010', '266001')\n" +
                "        ) \n" +
                "        THEN 1 \n" +
                "        ELSE 0 \n" +
                "      END\n" +
                "    ) AS cxdpnm_111,\n" +
                "    SUM(\n" +
                "      CASE\n" +
                "        WHEN (\n" +
                "          SUBSTR(a.itemcd, 1, 4) IN (\n" +
                "            '2320',\n" +
                "            '2330',\n" +
                "            '2340',\n" +
                "            '2350',\n" +
                "            '2360',\n" +
                "            '2370',\n" +
                "            '2015',\n" +
                "            '2310'\n" +
                "          ) \n" +
                "          OR SUBSTR(a.itemcd, 1, 6) IN ('217002', '262010', '266001')\n" +
                "        ) \n" +
                "        THEN a.lastbl * b.cvrate * nvl (a.instrt, 0) \n" +
                "        ELSE 0 \n" +
                "      END\n" +
                "    ) AS cxcvst_111,\n" +
                "    SUM(\n" +
                "      CASE\n" +
                "        WHEN (\n" +
                "          SUBSTR(a.itemcd, 1, 4) IN (\n" +
                "            '2110',\n" +
                "            '2120',\n" +
                "            '2130',\n" +
                "            '2620',\n" +
                "            '2010',\n" +
                "            '2011',\n" +
                "            '2020',\n" +
                "            '2012',\n" +
                "            '2040',\n" +
                "            '2041',\n" +
                "            '2042',\n" +
                "            '2043',\n" +
                "            '2050',\n" +
                "            '2060',\n" +
                "            '2013',\n" +
                "            '2030',\n" +
                "            '2230',\n" +
                "            '2610',\n" +
                "            '2210',\n" +
                "            '2160',\n" +
                "            '2220',\n" +
                "            '2611',\n" +
                "            '2630',\n" +
                "            '2161',\n" +
                "            '2090'\n" +
                "          ) \n" +
                "          OR SUBSTR(a.itemcd, 1, 6) IN (\n" +
                "            '219002',\n" +
                "            '207001',\n" +
                "            '266003',\n" +
                "            '211010',\n" +
                "            '219001'\n" +
                "          ) \n" +
                "          OR a.itemcd IN (\n" +
                "            '24400301',\n" +
                "            '24601001',\n" +
                "            '24601002'\n" +
                "          )\n" +
                "        ) \n" +
                "        THEN a.lastbl * b.cvrate \n" +
                "        ELSE 0 \n" +
                "      END\n" +
                "    ) AS cpdepo_111,\n" +
                "    SUM(\n" +
                "      CASE\n" +
                "        WHEN (\n" +
                "          SUBSTR(a.itemcd, 1, 4) IN (\n" +
                "            '2110',\n" +
                "            '2120',\n" +
                "            '2130',\n" +
                "            '2620',\n" +
                "            '2010',\n" +
                "            '2011',\n" +
                "            '2020',\n" +
                "            '2012',\n" +
                "            '2040',\n" +
                "            '2041',\n" +
                "            '2042',\n" +
                "            '2043',\n" +
                "            '2050',\n" +
                "            '2060',\n" +
                "            '2013',\n" +
                "            '2030',\n" +
                "            '2230',\n" +
                "            '2610',\n" +
                "            '2210',\n" +
                "            '2160',\n" +
                "            '2220',\n" +
                "            '2611',\n" +
                "            '2630',\n" +
                "            '2161',\n" +
                "            '2090'\n" +
                "          ) \n" +
                "          OR SUBSTR(a.itemcd, 1, 6) IN (\n" +
                "            '219002',\n" +
                "            '207001',\n" +
                "            '266003',\n" +
                "            '211010',\n" +
                "            '219001'\n" +
                "          ) \n" +
                "          OR a.itemcd IN (\n" +
                "            '24400301',\n" +
                "            '24601001',\n" +
                "            '24601002'\n" +
                "          )\n" +
                "        ) \n" +
                "        THEN 1 \n" +
                "        ELSE 0 \n" +
                "      END\n" +
                "    ) AS cpdpnm_111,\n" +
                "    SUM(\n" +
                "      CASE\n" +
                "        WHEN (\n" +
                "          SUBSTR(a.itemcd, 1, 4) IN (\n" +
                "            '2110',\n" +
                "            '2120',\n" +
                "            '2130',\n" +
                "            '2620',\n" +
                "            '2010',\n" +
                "            '2011',\n" +
                "            '2020',\n" +
                "            '2012',\n" +
                "            '2040',\n" +
                "            '2041',\n" +
                "            '2042',\n" +
                "            '2043',\n" +
                "            '2050',\n" +
                "            '2060',\n" +
                "            '2013',\n" +
                "            '2030',\n" +
                "            '2230',\n" +
                "            '2610',\n" +
                "            '2210',\n" +
                "            '2160',\n" +
                "            '2220',\n" +
                "            '2611',\n" +
                "            '2630',\n" +
                "            '2161',\n" +
                "            '2090'\n" +
                "          ) \n" +
                "          OR SUBSTR(a.itemcd, 1, 6) IN (\n" +
                "            '219002',\n" +
                "            '207001',\n" +
                "            '266003',\n" +
                "            '211010',\n" +
                "            '219001'\n" +
                "          ) \n" +
                "          OR a.itemcd IN (\n" +
                "            '24400301',\n" +
                "            '24601001',\n" +
                "            '24601002'\n" +
                "          )\n" +
                "        ) \n" +
                "        THEN a.lastbl * b.cvrate * nvl (a.instrt, 0) \n" +
                "        ELSE 0 \n" +
                "      END\n" +
                "    ) cpcvst_111,\n" +
                "    SUM(\n" +
                "      CASE\n" +
                "        WHEN SUBSTR(a.itemcd, 1, 4) = '2620' \n" +
                "        THEN a.lastbl * b.cvrate \n" +
                "        ELSE 0 \n" +
                "      END\n" +
                "    ) AS pudepo_111,\n" +
                "    SUM(\n" +
                "      CASE\n" +
                "        WHEN SUBSTR(a.itemcd, 1, 4) = '2620' \n" +
                "        THEN 1 \n" +
                "        ELSE 0 \n" +
                "      END\n" +
                "    ) AS pudpnm_111,\n" +
                "    SUM(\n" +
                "      CASE\n" +
                "        WHEN SUBSTR(a.itemcd, 1, 4) = '2620' \n" +
                "        THEN a.lastbl * b.cvrate * nvl (a.instrt, 0) \n" +
                "        ELSE 0 \n" +
                "      END\n" +
                "    ) pucvst_111 \n" +
                "  FROM\n" +
                "    dcam_ac_dp_acct_hs_ssd_1 a \n" +
                "    INNER JOIN dccm_cvrt b \n" +
                "      ON b.crcycd = a.crcycd \n" +
                "      AND b.uncrcy = 'CN' \n" +
                "      AND b.acctdt = DATE '2015-01-05' \n" +
                "    INNER JOIN dcpt_br_cobr c \n" +
                "      ON c.cobrno = a.brchno \n" +
                "      AND c.brchlv = '2' \n" +
                "  WHERE a.begndt <= DATE '2015-01-05' \n" +
                "    AND a.overdt > DATE '2015-01-05' \n" +
                "    AND a.lastbl <> 0 \n" +
                "    AND (\n" +
                "      SUBSTR(a.itemcd, 1, 4) IN (\n" +
                "        '2010',\n" +
                "        '2011',\n" +
                "        '2012',\n" +
                "        '2013',\n" +
                "        '2015',\n" +
                "        '2020',\n" +
                "        '2030',\n" +
                "        '2040',\n" +
                "        '2041',\n" +
                "        '2042',\n" +
                "        '2043',\n" +
                "        '2050',\n" +
                "        '2060',\n" +
                "        '2110',\n" +
                "        '2120',\n" +
                "        '2130',\n" +
                "        '2160',\n" +
                "        '2161',\n" +
                "        '2170',\n" +
                "        '2190',\n" +
                "        '2210',\n" +
                "        '2220',\n" +
                "        '2230',\n" +
                "        '2310',\n" +
                "        '2320',\n" +
                "        '2330',\n" +
                "        '2340',\n" +
                "        '2350',\n" +
                "        '2360',\n" +
                "        '2370',\n" +
                "        '2620',\n" +
                "        '2630',\n" +
                "        '2090',\n" +
                "        '2610',\n" +
                "        '2611'\n" +
                "      ) \n" +
                "      OR SUBSTR(a.itemcd, 1, 6) IN (\n" +
                "        '246003',\n" +
                "        '266001',\n" +
                "        '266003',\n" +
                "        '207001'\n" +
                "      ) \n" +
                "      OR a.itemcd IN (\n" +
                "        '24400301',\n" +
                "        '24601001',\n" +
                "        '24601002'\n" +
                "      )\n" +
                "    ) \n" +
                "  GROUP BY brchno_111,\n" +
                "    termcd_111) \n" +
                "ORDER BY 1 ";
        @Override
        public String createSql() {
            return String.format(template);
        }
    }
}
