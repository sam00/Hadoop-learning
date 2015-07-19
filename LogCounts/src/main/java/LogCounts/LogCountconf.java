package LogCounts;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import LogCounts.Counter_enum;

public class LogCountconf {

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {

    Path inputPath = new Path(args[0]);
    Path outputDir = new Path(args[1]);

    // Create configuration
    Configuration conf = new Configuration(true);

    // Create job
    @SuppressWarnings("deprecation")
    Job job = new Job(conf, "LogCountconf");
    job.setJarByClass(LogCountconf.class);

    // Setup MapReduce
    job.setMapperClass(LogCountMap.class);
    job.setReducerClass(LogCountReduce.class);
    job.setNumReduceTasks(1);

    // Specify key / value
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

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

    // Counter finding and displaying
    Counters counters = job.getCounters();

    // Displaying counters
    System.out.printf("Missing Fields: %d, Error Count: %d\n", counters
        .findCounter(Counter_enum.MISSING_FIELDS_RECORD_COUNT).getValue(),
        counters.findCounter(Counter_enum.NULL_OR_EMPTY).getValue());

    System.exit(code);

  }

}

