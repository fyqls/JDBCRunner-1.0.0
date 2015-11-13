package io.transwarp.sql.wuxi;

import io.transwarp.sql.SqlCreator;
import io.transwarp.sql.util.TestUtil;

/**
 * Created by gordonsong on 2/27/15.
 */
public abstract class WUXISQL implements SqlCreator {
    @Override
    public boolean isTpSql() {
        return true;
    }

    @Override
    public boolean needRunPreCmd() {
        return true;
    }


    public static class HYPE1 extends WUXISQL {

        static String template = "Select * from chengdao_inceptor where key.kkbh = '%s' and key.fxlx = '%s'";
        @Override
        public String createSql() {
            String kkbh = TestUtil.randomParameter(0);
            String fxlx = TestUtil.randomParameter(1);
            return String.format(template, kkbh, fxlx);
        }
    }

    public static class HYPE2 extends WUXISQL {

        static String template = "Select * from chengdao_inceptor where hphm = '%s' and dt <= '%s'";
        @Override
        public String createSql() {
            String hphm = TestUtil.randomParameter(2);
            String dt = TestUtil.randomParameter(3);
            return String.format(template, hphm, dt);
        }
    }

}
