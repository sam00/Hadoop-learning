package cloudwick.samples.uniquecount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniqueCountMapper1 extends Mapper<Object, Text, Text, IntWritable> {

  private final IntWritable alpha = new IntWritable(1);
  private Text gamma = new Text();

  public void map(Object key, Text value, Context context) throws IOException,
      InterruptedException {

    String[] csv = value.toString().split("::");

    if (csv.length > 0) {

      gamma.set(csv[0]);
      context.write(gamma, alpha);

    }
  }
}

