package hadoop.mapreduce.test.logstat3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

public class LogStatSortMapper extends Mapper<LongWritable,Text,LogItem,IntWritable> {

    private TreeMap<Integer, LogItem> treeMap = new TreeMap<Integer, LogItem>();

    protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
        String line=value.toString();
        String[] ss=line.split("\t");
        if(ss.length==2){
            LogItem item = new LogItem();
            item.setData(ss[0]);
            item.setCount(Integer.parseInt(ss[1]));
            context.write(item,new IntWritable(item.getCount()));
        }
    }

}