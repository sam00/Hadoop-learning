package temperature;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TempMapper extends
    Mapper<LongWritable, Text, Text, IntWritable> {

  private static final int MaxTemp = 9999;


  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    // checks here:

    String Line = value.toString();

    String[] recordSplits = Line.split("\t");
    String Year = recordSplits[0];

    int airTemperature;
    if (recordSplits.length == 3) {
      airTemperature = Integer.parseInt(recordSplits[1]);

      String quality = recordSplits[2];
      if (airTemperature >= MaxTemp && quality.matches("[01459]")) {
        context.write(new Text(Year), new IntWritable(airTemperature));
      }
    }

  }
}
