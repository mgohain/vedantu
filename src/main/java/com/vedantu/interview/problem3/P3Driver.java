package com.vedantu.interview.problem3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class P3Driver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new P3Driver(), args);
    }

    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(P3Driver.class);

        job.setMapperClass(P3Mapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(P3Reducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : -1;
    }
}
