package com.vedantu.interview.problem2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author mgohain
 * Created at 07/03/19
 */
public class P2Driver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new P2Driver(), args);
    }

    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(P2Driver.class);

        job.setMapperClass(P2Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(P2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : -1;
    }
}