package hadoop.mapreduce.test.logstat2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by xiaoweiliang on 2019/1/28.
 */
public class LogStatMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    protected void map(LongWritable key, Text value,Context context)
            throws IOException, InterruptedException {
        Pattern p = Pattern.compile("(\\d+\\.\\d+\\.\\d+\\.\\d+).*?\\d{4}:(\\d{2}):\\d{2}.*?");
        Matcher matcher = p.matcher(value.toString());
        while(matcher.find()){
            context.write(new Text("[" + matcher.group(2) + ":00]" + matcher.group(1)), new IntWritable(1));
        }
    }
}
