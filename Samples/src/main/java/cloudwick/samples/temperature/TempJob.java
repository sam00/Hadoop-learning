package cloudwick.samples.temperature;


/*
 * This exercise is about finding Max Temperature . It works like this
 * Mapper maps a line like this  [year] => [temp]. E.g.
 * 2000 => 32
 * 2000 => 31
 * 2000 => 24
 * 2001 => 29
 * 
 * Reducer reduces to [year]= max temperature. E.g.
 * 2000 => max (32, 31, 24)
 * 2001 => max(29);
 * 
 *  We are also checking if we can trust data by looking at quality value
 * */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TempJob {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage : MaxTemperature<input path> <output path>");
      System.exit(-1);
    }

    Path inputPath = new Path(args[0]);
    Path outputDir = new Path(args[1]);

    // Create configuration
    Configuration conf = new Configuration(true);

    // Create job
    Job job = new Job(conf, "TempJob");
    job.setJarByClass(TempJob.class);

    // Setup MapReduce
    job.setMapperClass(TempMapper.class);
    job.setReducerClass(TempReducer.class);
    // job.setNumReduceTasks(1);

    // Specify key / value
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // Input
    FileInputFormat.addInputPath(job, inputPath);
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

