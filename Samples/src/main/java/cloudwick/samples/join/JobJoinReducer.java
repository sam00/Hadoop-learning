package cloudwick.samples.join;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JobJoinReducer extends Reducer<Text, Text, Text, Text> {

  public void reduce(Text text, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    Iterator<Text> iter = values.iterator();
    Text song = new Text(iter.next());

    while (iter.hasNext()) {

      Text record = iter.next();
      Text outvalue = new Text(record.toString() + "\t" + song.toString());
      context.write(new Text(""), outvalue);
    }

  }
}
