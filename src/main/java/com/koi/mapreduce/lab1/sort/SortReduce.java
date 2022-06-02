package com.koi.mapreduce.lab1.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.checkerframework.checker.units.qual.A;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

public class SortReduce extends Reducer<IntWritable, IntWritable,IntWritable,IntWritable> {
    ArrayList<Integer>arr = new ArrayList();
    IntWritable intWritable = new IntWritable();
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //context.write(key,new Text(""));
        arr = new ArrayList<>();
        values.forEach(o->{
            arr.add(o.get());
        });
        arr.sort(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1-o2;
            }
        });
        for (int i=0;i<arr.size();i++){
            intWritable.set(i+1);
            context.write(intWritable,new IntWritable(arr.get(i)));
        }
    }
}
