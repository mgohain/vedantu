package com.vedantu.interview.problem2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author mgohain
 * Created at 07/03/19
 */
public class P2Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long userCount = 0;
        Iterator<LongWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            userCount = userCount + iterator.next().get();
        }
        context.write(key, new LongWritable(userCount));
    }
}
