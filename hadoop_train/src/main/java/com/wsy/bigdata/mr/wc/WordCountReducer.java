package com.wsy.bigdata.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * (hello ,1),(world,1)
 * (hello ,1),,(world,1)
 * (hello ,1),,(world,1)
 * map端的输出到reduce端，是按照相同的key分发到一个reduce上去执行
 * reduce1:(hello,1),(hello,1),(hello,1) ==>(hello,<1,1,1>)
 * reduce2:(world,1),(world,1),(world,1) ==>(world,<1,1,1>)
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        Iterator<IntWritable> iterator = values.iterator();
        //<1,1,1>
        while(iterator.hasNext()){
            IntWritable value = iterator.next();
            //value.get();会将IntWritable转换为java.lang.int
            count +=value.get();
        }
        //new IntWritable()将int转换为IntWritable
        context.write(key,new IntWritable(count));
    }
}
