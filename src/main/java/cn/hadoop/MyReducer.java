package cn.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @Author:renxin.tang
 * @Desc:
 * @Date: Created in 10:05 2019/3/21
 */
public class MyReducer extends Reducer<LongWritable, Text, Text, NullWritable> {
    protected void reduce(LongWritable k2, java.lang.Iterable<Text> v2s, org.apache.hadoop.mapreduce.Reducer<LongWritable,Text,Text,NullWritable>.Context context) throws java.io.IOException ,InterruptedException {
        for (Text v2 : v2s) {
            context.write(v2, NullWritable.get());
        }
    };
}
