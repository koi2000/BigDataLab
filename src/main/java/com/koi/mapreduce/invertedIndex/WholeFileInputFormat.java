package com.koi.mapreduce.invertedIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class WholeFileInputFormat extends FileInputFormat<Text,Text> {

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        RecordReader<Text,Text> recordReader = new WholeFileRecordReader();
        return recordReader;
    }

}