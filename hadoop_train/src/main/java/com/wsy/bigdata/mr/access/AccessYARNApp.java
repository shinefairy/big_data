package com.wsy.bigdata.mr.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Driver端
public class AccessYARNApp {
    public static void main(String[] args) throws Exception {
        Configuration configuration =new Configuration();

        Job job = Job.getInstance(configuration);

        job.setJarByClass(AccessYARNApp.class);

        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReducer.class);

        //设置自定义分区规则
        job.setPartitionerClass(AccessPartitioner.class);
        //设置reduce个数
        job.setNumReduceTasks(3);

        //设置Mapper的key value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);
        //设置Reducer的key value
        job.setOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Access.class);

        //设置文件的输入、输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交作业
        job.waitForCompletion(true);
    }
}
