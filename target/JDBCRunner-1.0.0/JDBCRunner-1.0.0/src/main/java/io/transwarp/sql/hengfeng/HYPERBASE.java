package io.transwarp.sql.hengfeng;

import io.transwarp.sql.SqlCreator;
import io.transwarp.sql.util.TestUtil;
import junit.framework.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by gordonsong on 2/27/15.
 */
public abstract class HYPERBASE implements SqlCreator {
    @Override
    public boolean isTpSql() {
        return true;
    }

    @Override
    public boolean needRunPreCmd() {
        return true;
    }

//    public static class HYPE0 extends HYPERBASE {
//
//        static String template = "select * from poc_tran_hy1 where rowkey.trandt = '%s' and rowkey.transq = '%s'";
//        @Override
//        public String createSql() {
//            String trandt = TestUtil.randomParameter(0);
//            String transq = TestUtil.randomParameter(1);
//            return String.format(template, trandt, transq);
//        }
//    }

    public static class HYPE1 extends HYPERBASE {

        static String template = "Select * from dcev_kl_tran_perf_hy where key.custno = '%s' and trandt between '%s' and '%s'";
        @Override
        public String createSql() {
            String custno = TestUtil.randomParameter(0);
            String trandt_start = TestUtil.randomParameter(1);
            String trandt_end = null;
            SimpleDateFormat f =  new SimpleDateFormat("yyyy-MM-dd");
            try   {
                Date d  =  new Date(f.parse(trandt_start).getTime()+24*3600*1000);
                trandt_end = f.format(d);
            }
            catch(Exception ex) {
                trandt_end = trandt_start;
            }

            return String.format(template, custno, trandt_start, trandt_end);
        }
    }
    

    public static class HYPE5 extends HYPERBASE {

        static String template = "Select * from dcev_kl_tran_perf_hy where key.custno = '%s' and trandt <= '%s' and trandt >= '%s'";
        @Override
        public String createSql() {
            String custno = TestUtil.randomParameter(0);
            String trandt = TestUtil.randomParameter(1);
            return String.format(template, custno, trandt, trandt);
        }
    }
    
    public static class HYPE6 extends HYPERBASE {

        static String template = "Select * from dcev_kl_tran_perf_hy where acctno = '%s' and trandt between '%s' and '%s'";
        @Override
        public String createSql() {
            String acctno = TestUtil.randomParameter(0);
            String trandt = TestUtil.randomParameter(1);
            return String.format(template, acctno, trandt, trandt);
        }
    }

    public static class HYPE2 extends HYPERBASE {

        static String template = "Select * from dcev_kl_tran_perf_hy where acctno = '%s' limit 10";
//        static String template = "Select * from dcev_kl_tran_perf_hy where acctno = '%s' and trandt <= '%s' and trandt >= '%s'";
        @Override
        public String createSql() {
            String acctno = TestUtil.randomParameter(3);
           // String trandt = TestUtil.randomParameter(1);
            return String.format(template, acctno);
        }
    }
    

    public static class HYPE3 extends HYPERBASE {

        static String template = "Select * from dcev_kl_tran_perf_hy where toacct = '%s' limit 1";
        @Override
        public String createSql() {
            String toacct = TestUtil.randomParameter(2);
            return String.format(template, toacct);
        }
    }

    public static class HYPE4 extends HYPERBASE {

        static String template = "SELECT a.acctno, d.tranam, lastbl, crcycd, a.brchno, c.brchna, b.custno,b.custna, custst, idtfno, nation FROM dcam_ac_dp_acct_perf_hy a INNER JOIN dcpt_cp_cust_perf_hy b ON a.custno = b.custno INNER JOIN dcpt_br_brch_hy c ON a.brchno = c.brchno INNER JOIN dcev_kl_tran_perf_hy d ON d.key.custno = a.custno WHERE a.custno= '%s'";
        @Override
        public String createSql() {
            String custno = TestUtil.randomParameter(0);
            return String.format(template, custno);
        }
    }
}
