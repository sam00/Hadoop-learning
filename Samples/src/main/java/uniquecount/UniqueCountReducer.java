package uniquecount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UniqueCountReducer extends Reducer<Text, IntWritable, Text, Text> {

  public void reduce(Text text, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

    context.write(text, new Text(""));
    }
}