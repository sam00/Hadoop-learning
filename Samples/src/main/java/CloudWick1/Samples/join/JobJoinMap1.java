package CloudWick1.Samples.join;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JobJoinMap1 extends Mapper<Object, Text, Text, Text> {

  private final int StateIndex = 3;

  private final int TrackidIndex = 2;

  String seek = "night";
  String seperator = "<SEP>";

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    String[] splits = line.toString().split(seperator);

    if (splits.length == StateIndex + 1) {

      Boolean containsSearchword =
          splits[StateIndex].toLowerCase().contains(seek);

      String Trackid = splits[TrackidIndex];
      String Artistname = splits[StateIndex];

      // Filter
      if (containsSearchword)
        context.write(new Text(Trackid), new Text(Trackid + "<SEP>"
            + Artistname));

    }
  }
}
