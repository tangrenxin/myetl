package cn.hadoop;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * @Author:renxin.tang
 * @Desc:日志分析器
 * @Date: Created in 10:05 2019/3/21
 */
public class LogParser {
    public static final SimpleDateFormat FORMAT = new SimpleDateFormat("d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
    public static final SimpleDateFormat DATEFORMAT=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static void main(String[] args) throws ParseException {
		final String TEST = "27.19.74.143 - - [5/Mar/2019:17:38:20 +0800] \"GET /static/image/common/faq.gif HTTP/1.1\" 200 1127";
		LogParser parser = new LogParser();
		final String[] array = parser.parse(TEST);
		System.out.println("测试数据： "+TEST);
		System.out.format("解析结果：  ip=%s, time=%s, url=%s, status=%s, traffic=%s ,hour=%s", array[0], array[1], array[2], array[3], array[4] ,array[5]);
	}
    /**
     * 解析日志的行记录
     * @param line
     * @return 数组含有6个元素，分别是ip、时间、url、状态、流量、小时
     */
    public String[] parse(String line){
        String ip = parseIP(line);
        String time;
        try {
            time = parseTime(line);
        } catch (Exception e1) {
            time = "null";
        }
        String url;
        try {
            url = parseURL(line);
        } catch (Exception e) {
            url = "null";
        }
        String status = parseStatus(line);
        String traffic = parseTraffic(line);
        String hour = String.valueOf(parseHour(line));

        return new String[]{ip, time ,url, status, traffic,hour};
    }

    /**
     *流量
     * @param line
     * @return
     */
    public String parseTraffic(String line) {
        final String trim = line.substring(line.lastIndexOf("\"")+1).trim();
        String traffic = trim.split(" ")[1];
        return traffic;
    }

    /**
     * 状态
     * @param line
     * @return
     */
    public String parseStatus(String line) {
        String trim;
        try {
            trim = line.substring(line.lastIndexOf("\"")+1).trim();
        } catch (Exception e) {
            trim = "null";
        }
        String status = trim.split(" ")[0];
        return status;
    }

    /**
     * url
     * @param line
     * @return
     */
    public String parseURL(String line) {
        final int first = line.indexOf("\"");
        final int last = line.lastIndexOf("\"");
        String url = line.substring(first+1, last);
        return url;
    }

    /**
     * logtime
     * @param line
     * @return
     */
    public String parseTime(String line) {
        final int first = line.indexOf("[");
        final int last = line.indexOf("+0800]");
        String time = line.substring(first+1,last).trim();
        try {
            return DATEFORMAT.format(FORMAT.parse(time));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * hour
     * @param line
     * @return
     */
    public int parseHour(String line) {
        String datetime = parseTime(line);
        Timestamp ts = Timestamp.valueOf(datetime);
        Date date = new Date(ts.getTime());
        int hour = date.getHours();
        return hour;
    }

    /**
     * IP
     * @param line
     * @return
     */
    public String parseIP(String line) {
        String ip = line.split("- -")[0].trim();
        return ip;
    }
}
