package SecondarySortJob;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class ImpCounterMap extends Mapper<Object, Text, Text, DoubleWritable> {

  private Logger logger = Logger.getLogger("FilterMapper");

  private final int incomeIndex = 54;
  private final int countryIndex = 0;
  private final int lenIndex = 58;

  String seperator = ",";

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    if (line == null | !!line.toString().isEmpty()) {
      logger.info("null found.");
      context.getCounter(Counter_enum.NULL_OR_EMPTY).increment(1);
      return;
    }
    if (line.toString().contains(
        "Adjusted net national income per capita (current US$)")) {
      String[] recordSplits = line.toString().split(seperator);

      logger.info("splitted.");

      if (recordSplits.length == lenIndex) {

        String countryName = recordSplits[countryIndex];
        try {

          double income = Double.parseDouble(recordSplits[incomeIndex]);

          context.write(new Text(countryName), new DoubleWritable(income));

        } catch (NumberFormatException nfe) {

          logger.info("The value of income is in wrong format. " + countryName);
          context.getCounter(Counter_enum.MISSING_FIELDS_RECORD_COUNT)
              .increment(1);
          return;
        }

      }
    }
  }
}
