package CloudWick1.Samples.chaining;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChainedMapper1 extends Mapper<Object, Text, Text, Text> {

  private final int trackIdIndex = 0;
  private final int artistNameIndex = 1;
  private final int titleIndex = 3;
  private final int expectedLineLength = 4;

  String seek = "night";
  String seperator = "<SEP>";

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    if (line == null)
      return;

    String[] splits = line.toString().split(seperator);

    if (splits.length == expectedLineLength) {

      Boolean containsSearchword =
          splits[titleIndex].toLowerCase().contains(seek);

      String Trackid = splits[trackIdIndex];
      String Artistname = splits[artistNameIndex];
      String Title = splits[titleIndex];

      // it title contains search word
      if (containsSearchword)
        context.write(new Text(Trackid), new Text(Artistname + "\t" + Title));
    }
  }
}
