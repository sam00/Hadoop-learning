package uniquecount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniqueCountMapper1 extends Mapper<Object, Text, Text, IntWritable> {

  private final IntWritable alpha = new IntWritable(1);
  private Text gamma = new Text();

  public void map(Object key, Text value, Context context) throws IOException,
      InterruptedException {

    String[] file = value.toString().split(">>");

    if (file.length > 0) {

      gamma.set(file[0]);
      context.write(gamma, alpha);

    }
  }
}

