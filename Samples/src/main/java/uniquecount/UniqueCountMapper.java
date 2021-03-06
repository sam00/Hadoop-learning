package uniquecount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniqueCountMapper extends Mapper<Object, Text, Text, IntWritable> {
  
  private final IntWritable alpha = new IntWritable(1);
  private Text gamma = new Text();

  public void map(Object key, Text value, Context context) throws IOException,
      InterruptedException {

    String[] file = value.toString().split("\t");
    if (file.length == 2) {

      String userid = file[0];
      String Url = file[1];

      String pair = Url + ">>" + userid;

      gamma.set(pair);
      context.write(gamma, alpha);
    }
  }

}

