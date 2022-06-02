package com.koi.mapreduce.pageRank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * @author koi
 * @date 2022/4/27 10:20
 */
public class LastReduce extends Reducer<Text, Text, Text, Text> {

    Text text = new Text();
    Text value = new Text("");

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<pair> list = new ArrayList<>();
        for (Text value : values) {
            String val = value.toString();
            String[] split = val.split(":");
            String k = split[0];
            String v = split[1];
            list.add(new pair(k, Double.parseDouble(v)));
        }

        list.sort((o1, o2) -> {
            if (o1.v > o2.v) {
                return -1;
            } else {
                return 1;
            }
        });
        for (pair pair : list) {
            //BigDecimal bg = BigDecimal.valueOf(pair.v);
            //double d = bg.setScale(10, BigDecimal.ROUND_HALF_UP).doubleValue();
            String format = new DecimalFormat("#,##0.0000000000").format(pair.v);
//            String str = d.toString();
//            if (str.contains(".")) {
//                str = str.replaceAll("0+$", "");
//                str = str.replaceAll("[.]$", "");
//            }
            String res = "(" + pair.k + "," + format + ")";
            text.set(res);
            context.write(text, value);
        }
    }


    static class pair {
        public String k;
        public Double v;

        public pair(String k, Double v) {
            this.k = k;
            this.v = v;
        }

    }
}
