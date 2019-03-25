package cn.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * @Author:renxin.tang
 * @Desc:
 * @Date: Created in 09:30 2019/3/21
 */
public class LogMR extends Configured implements Tool{

    public int run(String[] args) throws Exception {
        final String inputPath = args[0];
        final String outPath = args[1];

        final Configuration conf = new Configuration();
        final Job job = new Job(conf, LogMR.class.getSimpleName());
        job.setJarByClass(LogMR.class);

        FileInputFormat.setInputPaths(job, inputPath);
        //map
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        //reduce
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        job.waitForCompletion(true);
        return 0;
    }
    public static void main(String[] args)  throws Exception{
        ToolRunner.run(new LogMR(), args);
    }

}

