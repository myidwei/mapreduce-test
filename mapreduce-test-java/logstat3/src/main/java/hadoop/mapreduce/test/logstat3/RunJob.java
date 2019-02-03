package hadoop.mapreduce.test.logstat3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class RunJob
{
    private static void runJob(String in,String out){
        Configuration config = new Configuration();
        try {
            FileSystem fs = FileSystem.get(config);
            Job job = Job.getInstance(config);
            job.setJobName("LogStat3");
            job.setJarByClass(RunJob.class);
            job.setMapperClass(LogStatMapper.class);
            job.setReducerClass(LogStatReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(in));
            Path outPath = new Path(out);
            if(fs.exists(outPath)){
                fs.delete(outPath, true);
            }
            FileOutputFormat.setOutputPath(job, outPath);
            Boolean result = job.waitForCompletion(true);
            if(result){
                System.out.println("Job is complete!");
            }else{
                System.out.println("Job is fail!");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void runGroupSortJob(String in,String out){

        Configuration config = new Configuration();
        try {
            FileSystem fs = FileSystem.get(config);
            Job job = Job.getInstance(config);
            job.setJobName("LogStatGroupSort3");
            job.setJarByClass(RunJob.class);
            job.setMapperClass(LogStatSortMapper.class);
            job.setReducerClass(LogStatSortReducer.class);
            job.setMapOutputKeyClass(LogItem.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setPartitionerClass(HourPartition.class);
            job.setNumReduceTasks(24);
            job.setSortComparatorClass(SortItem.class);
            FileInputFormat.addInputPath(job, new Path(in));
            Path outPath = new Path(out);
            if(fs.exists(outPath)){
                fs.delete(outPath, true);
            }
            FileOutputFormat.setOutputPath(job, outPath);
            Boolean result = job.waitForCompletion(true);
            if(result){
                System.out.println("Sort Job is complete!");
            }else{
                System.out.println("Sort Job is fail!");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }




    public static void main(String[] args) {
        runJob("/logstat3/input","/logstat3/temp");
        runGroupSortJob("/logstat3/temp","/logstat3/output");
    }
}
