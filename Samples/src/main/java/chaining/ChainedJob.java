package chaining;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/*
 * This exercise is about foo and bar. It works like this
 * - Map Mapper1 to Mapper2
 * - 
 * 
 * 
 * Input is this
 * Unique_artist
 * Unique_tracks
 * 
 * Output is that
 * 
 * String related to night
 * 
 * */
public class ChainedJob {

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {

    Path inputPath1 = new Path(args[0]);
    // Path inputPath2 = new Path(args[1]);
    Path outputDir = new Path(args[1]);

    // Create configuration
    Configuration conf = new Configuration(true);

    // Create job
    Job job = new Job(conf, "ChainedJob");
    job.setJarByClass(ChainedJob.class);

    // config1

    Configuration conf2 = new Configuration(true);

    // Create job1

    Job job1 = new Job(conf2, "ChainedJob");
    job1.setJarByClass(ChainedJob.class);


    // Setup MapReduce
    job.setMapperClass(ChainedMapper1.class);
    job.setNumReduceTasks(1);

    // Setup MapReduce1
    job1.setMapperClass(ChainedMapper2.class);
    job1.setNumReduceTasks(1);

    // Specify key / value
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Specify key / value 1
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);

    // Input
    FileInputFormat.addInputPath(job, inputPath1);
    job.setInputFormatClass(TextInputFormat.class);

    // Input1
    FileInputFormat.addInputPath(job1, inputPath1);
    job1.setInputFormatClass(TextInputFormat.class);

    // Output
    FileOutputFormat.setOutputPath(job, outputDir);
    job.setOutputFormatClass(TextOutputFormat.class);

    // Output1
    FileOutputFormat.setOutputPath(job1, outputDir);
    job1.setOutputFormatClass(TextOutputFormat.class);

    // Delete output if exists
    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(outputDir))
      hdfs.delete(outputDir, true);

    // Execute job
    int code = job.waitForCompletion(true) ? 0 : 1;
    System.exit(code);

    // Execute job1
    int code1 = job1.waitForCompletion(true) ? 0 : 1;
    System.exit(code1);

  }

}



