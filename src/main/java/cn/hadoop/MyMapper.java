package cn.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @Author:renxin.tang
 * @Desc:
 * @Date: Created in 10:07 2019/3/21
 */
public class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    LogParser ps = new LogParser();

    Text v2 = new Text();
    protected void map(LongWritable key, Text value, Mapper<LongWritable,Text,LongWritable,Text>.Context context) throws java.io.IOException ,InterruptedException {
        final String line = value.toString();
        final String[] parsed = ps.parse(line);
        final String ip =ps.parseIP(line);
        final String logtime = ps.parseTime(line);
        final String hour = String.valueOf(ps.parseHour(line));
        String url = ps.parseURL(line);

        //过滤所有静态的资源请求
        if(url.startsWith("GET /static")||url.startsWith("GET /uc_server")){
            return;
        }
        //  去掉GET 和Post
        if(url.startsWith("GET")){
            url = url.substring("GET ".length()+1, url.length()-" HTTP/1.1".length());
        }
        if(url.startsWith("POST")){
            url = url.substring("POST ".length()+1, url.length()-" HTTP/1.1".length());
        }
        v2.set(ip+"\t"+logtime +"\t"+url+"\t"+hour);
        context.write(key, v2);
    };
}
