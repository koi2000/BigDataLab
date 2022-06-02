package com.koi.mapreduce.invertedIndex;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class WholeFileRecordReader extends RecordReader<Text,Text> {

    private FileSplit fileSplit;
    private JobContext jobContext;
    private Text currentKey = new Text();
    private Text currentValue = new Text();
    private boolean finishConverting = false;
    @Override
    public void close() throws IOException {
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public Text getCurrentValue() throws IOException,
            InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        float progress = 0;
        if(finishConverting){
            progress = 1;
        }
        return progress;
    }

    @Override
    public void initialize(InputSplit arg0, TaskAttemptContext arg1)
            throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) arg0;
        this.jobContext = arg1;
        String filename = fileSplit.getPath().getName();
        this.currentKey = new Text(filename);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!finishConverting){
            int len = (int)fileSplit.getLength();
//          byte[] content = new byte[len];
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(jobContext.getConfiguration());
            FSDataInputStream in = fs.open(file);
            BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
//          BufferedReader br = new BufferedReader(new InputStreamReader(in,"utf-8"));
            String line="";
            String total="";
            while((line= br.readLine())!= null){
                total =total+line+"\n";
            }
            br.close();
            in.close();
            fs.close();
            currentValue = new Text(total);
            finishConverting = true;
            return true;
        }
        return false;
    }

}