package io.transwarp.sql.example;

import io.transwarp.sql.SqlCreator;
import io.transwarp.sql.util.TestUtil;

import java.util.Random;

/**
 * Created by gordonsong on 2/27/15.
 */
public abstract class TEST implements SqlCreator {
    @Override
    public boolean isTpSql() {
        return true;
    }

    @Override
    public boolean needRunPreCmd() {
        return true;
    }

    public static class TEST1 extends TEST {

        @Override
        public String createSql() {
            return getSql();
        }

        public static String getSql() {
            Random r = new Random();
            int idx = r.nextInt(ExampleUtil.sqlList.size());
            String template = ExampleUtil.sqlList.get(idx);
            int idx2 = r.nextInt(99);
            String key = "a"+String.format("%02d",idx2);
            if(idx == 0){
                return String.format(template, key);
            }else if((idx == 7)){
                String v2 = String.valueOf((float)idx2);
                String v3 = String.valueOf(idx2);
                return String.format(template, key,v2,v3);
            }else if( (idx==1) || (idx==5) ){
                String val = String.valueOf((double)idx2);
                return String.format(template, key, val);
            }else if((idx==2) || (idx==6)){
                String val = String.valueOf((float)idx2);
                return String.format(template, key, val);
            }else if((idx==3) || (idx==8)){
                String val = String.valueOf(idx2);
                return String.format(template, key, val);
            }else {
                String val = "d"+String.format("%02d",idx2);
                return String.format(template, key, val);
            }
        }

    }
    


}
