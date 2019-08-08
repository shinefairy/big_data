package com.wsy.bigdata.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * 使用MR统计HDFS上的文件对应的词频
 * Driver：配置Mapper,Reduce的相关属性
 * 提交到本地运行，开发过程中使用
 */
public class WordCountApp {
    public static void main(String[] args)throws Exception {
        //创建一个Job
        System.setProperty("HADOOP_USER_NAME","root");
        Configuration configuration =new Configuration();
        //记录一个error：CDH中hdfs端口为8082
        configuration.set("fs.defaultFS","hdfs://master1:9000");
        //创建一个Job
        Job job = Job.getInstance(configuration);

        //设置Job对应的参数：主类
        job.setJarByClass(WordCountApp.class);

        //设置Job对应的参数：设置自定义的Mapper和Reduce处理类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //设置Job对应的参数：Mapper输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置Job对应的参数：Reducer输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //对输出目录进行优化
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://master1:9000"), configuration);
        Path outputPath =new Path("/wordcount/output");
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath,true);
        }
        //设置Job对应的参数：Mapper输出key和value的类型：作业输入和输出的路径
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job,outputPath);

        //提交作业
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0:-1);

    }
}
