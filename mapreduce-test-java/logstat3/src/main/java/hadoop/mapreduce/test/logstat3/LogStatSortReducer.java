package hadoop.mapreduce.test.logstat3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
public class LogStatSortReducer extends Reducer<LogItem,IntWritable,LogItem,IntWritable> {

    @Override
    protected void reduce(LogItem item, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable value : values){
            context.write(item,value);
        }
    }

}