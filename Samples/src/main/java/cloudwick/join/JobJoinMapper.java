package cloudwick.join;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JobJoinMapper extends Mapper<Object, Text, Text, Text> {

  public void map(Object key, Text value, Context context) throws IOException,
      InterruptedException {

    String[] splitted = value.toString().split("<SEP>");
    String searchWord = "night";
    int titleIndex = 3;

    if (splitted.length == titleIndex + 1) {

      boolean containsSearchWord =
          splitted[titleIndex].toLowerCase().contains(searchWord);
      if (containsSearchWord) {

        context.write(new Text(), new Text(value));
      }

    }
  }
}
