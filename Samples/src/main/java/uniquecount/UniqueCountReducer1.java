package uniquecount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UniqueCountReducer1 extends
    Reducer<Text, IntWritable, Text, IntWritable> {

  public void reduce(Text text, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

    int add = 0;
    for (IntWritable value : values) {
      add += value.get();
    }
    context.write(text, new IntWritable(add));
  }
}

