package cloudwick.samples.uniquecount;

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

import cloudwick.samples.chaining.ChainedJob;
import cloudwick.samples.chaining.ChainedMapper1;

public class UniqueCount {

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {

    Path inputPath1 = new Path(args[0]);
    // Path inputPath2 = new Path(args[1]);
    Path outputDir = new Path(args[1]);

    // Create configuration
    Configuration conf = new Configuration(true);

    // Create job
    Job job = new Job(conf, "UniqueCount");
    job.setJarByClass(ChainedJob.class);

    // Setup MapReduce
    job.setMapperClass(ChainedMapper1.class);
    job.setNumReduceTasks(1);

    // Specify key / value
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Input
    FileInputFormat.addInputPath(job, inputPath1);
    job.setInputFormatClass(TextInputFormat.class);

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
