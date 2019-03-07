package com.vedantu.interview.problem1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author mgohain
 * Created at 07/03/19
 */
public class P1Reducer extends Reducer<LongWritable, LongWritable, LongWritable, DoubleWritable> {
    public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        long sessionCount = 0;
        Iterator<LongWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            sum = sum + iterator.next().get();
            sessionCount++;
        }
        double avg = sum/sessionCount;
        context.write(key, new DoubleWritable(avg));
    }
}
