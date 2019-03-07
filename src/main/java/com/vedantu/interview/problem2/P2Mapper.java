package com.vedantu.interview.problem2;

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
public class P2Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private Text myKey = new Text();
    private LongWritable myValue = new LongWritable();
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        JSONArray objects = new JSONArray(value.toString());
        Iterator<Object> itr = objects.iterator();
        while (itr.hasNext()) {
            JSONObject object = (JSONObject) itr.next();
            String region = object.getString("region");
            if (null != region && !region.equals("-")) {
                myKey.set(region);
                myValue.set(object.getInt("calc_userid"));
                context.write(myKey, myValue);
            }
        }
    }
}
