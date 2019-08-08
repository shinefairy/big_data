package com.wsy.bigdata.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN :Map任务读数据的key类型,offset,是每行数据起始位置的偏移量，Long
 *VALUEIN:Map任务读数据的value类型，其实就是一行行的字符串，String
 *
 * 词频统计：相同单词的次数统计 （word ,1）
 * hello world welcome
 * hello world
 *
 *
 *
 * KEYOUT：Map方法自定义输出的key类型
 * VALUEOUT：Map方法自定义输出的value类型
 */
public class WordCountMapper extends Mapper<LongWritable, Text ,Text,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //把value对应的行数据按照指定的分隔符进行切分
        String[] words = value.toString().split(" ");
        for(String word:words){
            //(hello ，1)，（world，1） 优化，大小写全部转换为小写统计
            context.write(new Text(word.toLowerCase()),new IntWritable(1));
        }
    }
}
