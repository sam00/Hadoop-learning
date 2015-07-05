package CloudWick1.Samples.join;

import java.io.IOException;

/*
 * This exercise is about foo and bar. It works like this
 * - Map mapper  to map1
 * - Map track id and artistid
 * - 
 * 
 * Input is this
 *Unique_tracks and unique_artist 
 * 
 *
 * Output is that
 * Strings related to artist and trackid
 * 
 * */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class JobJoin {
  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {

    Path inputPath1 = new Path(args[0]);
    Path inputPath2 = new Path(args[1]);
    Path outputDir = new Path(args[2]);

    // Create configuration
    Configuration conf = new Configuration(true);

    // Create job
    Job job = new Job(conf, "JobJoin");
    job.setJarByClass(JobJoinMapper.class);

    // Setup MapReduce
//    job.setMapperClass(JobJoinMapper.class);
//    job.setReducerClass(JobJoinReducer.class);
    job.setNumReduceTasks(1);

    // Specify key / value
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Input

    MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, JobJoinMapper.class);
    MultipleInputs.addInputPath(job, inputPath2, TextInputFormat.class,
        JobJoinMap1.class);

    // FileInputFormat.addInputPath(job, inputPath);
    // job.setInputFormatClass(TextInputFormat.class);

    // Output
    FileOutputFormat.setOutputPath(job, outputDir);
    job.setOutputFormatClass(TextOutputFormat.class);

    // Delete output if exists
    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(outputDir))
      hdfs.delete(outputDir, true);

    // Execute job
    int code = job.waitForCompletion(true) ? 0 : 1;
    System.exit(code);

  }

}
