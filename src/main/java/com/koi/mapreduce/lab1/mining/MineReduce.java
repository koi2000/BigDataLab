package com.koi.mapreduce.lab1.mining;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class MineReduce extends Reducer<Text, Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //context.write(key,new Text(""));
        ArrayList<String>child = new ArrayList<>();
        ArrayList<String>parent = new ArrayList<>();
        values.forEach(o->{
            String str = o.toString();
            if(str.charAt(0)=='1'){
                parent.add(str.substring(1));
            }else{
                child.add(str.substring(1));
            }
        });

        for (int i=0;i<child.size();i++){
            for (int j=0;j<parent.size();j++){
                context.write(new Text(child.get(i)),new Text(parent.get(j)));
            }
        }
    }
}
