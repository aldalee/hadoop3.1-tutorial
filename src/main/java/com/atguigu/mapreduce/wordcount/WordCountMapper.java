package com.atguigu.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author HuanyuLee
 * @date 2023/6/23
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text outK = new Text();
    IntWritable outV = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            outK.set(word);
            outV.set(1);
            ctx.write(outK, outV);
        }
    }
}
