package com.vedantu.interview.problem1;


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
public class P1Mapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
    private LongWritable myKey = new LongWritable();
    private LongWritable myValue = new LongWritable();
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        JSONArray objects = new JSONArray(value.toString());
        Iterator<Object> itr = objects.iterator();
        while (itr.hasNext()) {
            JSONObject object = (JSONObject) itr.next();
            if (object.getInt("appnameenc") == 1 || object.getInt("appnameenc") == 2) {
                myKey.set(object.getLong("sessionid"));
                //I have not found start timestamp and stop timestamp. So treating this as the value
                myValue.set(object.getLong("timestamp"));
                context.write(myKey, myValue);
            }

        }
    }
}
