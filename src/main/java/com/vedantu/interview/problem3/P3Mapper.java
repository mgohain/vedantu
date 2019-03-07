package com.vedantu.interview.problem3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author mgohain
 * Created at 07/03/19
 */
public class P3Mapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
    private LongWritable myKey = new LongWritable();
    private IntWritable myValue = new IntWritable();
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        JSONArray objects = new JSONArray(value.toString());
        Iterator<Object> itr = objects.iterator();
        while (itr.hasNext()) {
            JSONObject object = (JSONObject) itr.next();
            long eventlaenc = object.getLong("eventlaenc");
            if (eventlaenc == 107 || eventlaenc == 126) {
                myKey.set(object.getLong("calc_userid"));
                if (eventlaenc==107) {
                    myValue.set(107);
                } else {
                    myValue.set(126);
                }
                context.write(myKey, myValue);
            }

        }
    }
}
