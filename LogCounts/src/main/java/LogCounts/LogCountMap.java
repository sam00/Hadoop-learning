package LogCounts;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;



public class LogCountMap extends Mapper<Object, Text, Text, IntWritable> {

  private final int LenghtIndex = 9;
  private final int StatusIn = 8;
  
  private Logger logger = Logger.getLogger("FilterMapper");
  // String seek = "404, 503, 200";
  String seperator = " ";
  
  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    if (line == null | line.toString().isEmpty()) {
      return;
    }

    String[] splits = line.toString().split(seperator);
    
    logger.info("wrong format. ");

    if (splits.equals(LenghtIndex)) {
    String StatusCode = splits[StatusIn];

       if (StatusCode.length() > 3 || StatusCode.length() < 2) {
       context.getCounter(Counter_enum.MISSING_FIELDS_RECORD_COUNT).increment(1);
      
       return;
      }

    if (StatusCode.matches("200")) {
      context.getCounter(Counter_enum.StatusCode200).increment(1);

    } else if (StatusCode.matches("503")) {
      context.getCounter(Counter_enum.StatusCode503).increment(1);

    } else if (StatusCode.matches("404")) {
      context.getCounter(Counter_enum.StatusCode404).increment(1);
    } else
            context.getCounter(Counter_enum.MISSING_FIELDS_RECORD_COUNT).increment(1);
        
        return;
        }
  }
}
 

