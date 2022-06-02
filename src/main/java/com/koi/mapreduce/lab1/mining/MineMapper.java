package com.koi.mapreduce.lab1.mining;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

public class MineMapper extends Mapper<Object,Text,Text,Text> {
    private static Text kk = new Text();
    private static Text vv = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split(" ");
        kk = new Text(split[0]);
        vv = new Text(split[1]);
        //1代表parent
        //0代表child
        context.write(kk,new Text("1"+split[1]));
        context.write(vv,new Text("0"+split[0]));
    }
}
