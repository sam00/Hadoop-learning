package ProjectCounter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class ImpCounter {

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {

    Path inputPath = new Path(args[0]);
    Path outputDir = new Path(args[1]);

    // Create configuration
    Configuration conf = new Configuration(true);

    // Create job
    @SuppressWarnings("deprecation")
    Job job = new Job(conf, "ImpCounter");
    job.setJarByClass(ImpCounter.class);

    // Decompressing .gz file Ex. foo.csv.gz to foo.csv

    String uri = args[0];
    FileSystem fs = FileSystem.get(URI.create(uri), conf);

    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    CompressionCodec codec = factory.getCodec(inputPath);
    if (codec == null) {
      System.err.println("No codec found for " + uri);
      System.exit(1);
    }
    String outputUri =
        CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
    InputStream in = null;
    OutputStream out = null;
    try {
      in = codec.createInputStream(fs.open(inputPath));
      out = fs.create(new Path(outputUri));
      IOUtils.copyBytes(in, out, conf);
    } finally {
      IOUtils.closeStream(in);
      IOUtils.closeStream(out);
}

    // Setup MapReduce
    job.setMapperClass(ImpCounterMap.class);
    job.setPartitionerClass(BasicPartitioner.class);

    job.setGroupingComparatorClass(BasicGrouping.class);
    job.setSortComparatorClass(BasicCompKeySort.class);
    job.setReducerClass(CountryIncomeReducer.class);
    job.setNumReduceTasks(1);

    // Specify key / value
    job.setOutputKeyClass(Compositekeywrite.class);
    job.setOutputValueClass(NullWritable.class);

    // Input
    // FileInputFormat.addInputPath(job, inputPath);
    FileInputFormat.addInputPaths(job, outputUri);
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