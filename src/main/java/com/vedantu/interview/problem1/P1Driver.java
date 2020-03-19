package com.vedantu.interview.problem1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
public class P1Driver extends Configured implements Tool {
    //Testing local git branching through terminal
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new P1Driver(), args);
    }
    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(P1Driver.class);

        job.setMapperClass(P1Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(P1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : -1;
    }
}
