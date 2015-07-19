package LogCounts;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

import ProjectCounter.Compositekeywrite;
import ProjectCounter.Counter_enum;

public class LogCountMap extends Mapper<Object, Text, Text, Text> {

  private final int TitleIndex = 3;
  private final int ArtistIndex = 2;
  private final int TrackIdIndex = 0;
  private final int ExpectedSplitIndex = 4;

  // String seek = "night";
  String seperator = "<SEP>";
  String outSeperator = "\t";

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    if (line == null) {
      return;
    }

    String[] splits = line.toString().split(seperator);

    if (splits.length == ExpectedSplitIndex) {

      // Search for the keyword
      // Boolean containsSearchword =
      // splits[TitleIndex].toLowerCase().contains(seek);

      String Trackid = splits[TrackIdIndex];
      String Artistname = splits[ArtistIndex];
      String Title = splits[TitleIndex];

      // Map if keyword found
      // if (containsSearchword

      // logger.info("The value of income is in wrong format. ");
        context.getCounter(Counter_enum.MISSING_FIELDS_RECORD_COUNT).increment(
            1);
        return;
        }
      }
    }
 

