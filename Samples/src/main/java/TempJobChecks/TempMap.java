package TempJobChecks;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TempMap extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  int yearIndex = 0;
  int expectLength = 7;
  int tempIndex = 2;

  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    // checks here
    String line = value.toString();

    if (line == null || line.isEmpty()) {
      return;
    }

    String[] recordSplits = line.trim().split(" +");
    if (recordSplits.length == expectLength) {


      String year = recordSplits[yearIndex];

      try{
        double temp = Double.parseDouble(recordSplits[tempIndex]);
        context.write(new Text(year), new DoubleWritable(temp));
      }
      catch(NumberFormatException ex)
      {
        // this is not a number, just ignore this record
      }
    }
  }
}
