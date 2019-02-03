package hadoop.mapreduce.test.logstat3;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class HourPartition extends Partitioner<LogItem,IntWritable> {
    @Override
    public int getPartition(LogItem key, IntWritable value, int num) {
        String data = key.getData();
        String[] array = data.split("\\|");
        String hour = array[0];
        return Integer.parseInt(hour);
    }
}