package com.vedantu.interview.problem3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author mgohain
 * Created at 07/03/19
 */
public class P3Reducer extends Reducer<LongWritable, IntWritable, LongWritable, Text> {
    public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> iterator = values.iterator();
        int count107 = 0;
        int count126 = 0;
        while (iterator.hasNext()) {
            if (iterator.next().get()==107) {
                count107++;
            } else {
                count126++;
            }
        }
        context.write(key, new Text("126 | " + count126 + " | 107 | " + count107));
    }
}
