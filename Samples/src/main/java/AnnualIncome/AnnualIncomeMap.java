package AnnualIncome;


import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class AnnualIncomeMap extends Mapper<Object, Text, Text, DoubleWritable> {

  private Logger logger = Logger.getLogger("FilterMapper");

  private final int lengthIndex = 58;
  private final int incomeIndex = 54;
  private final int countryIndex = 0;

  String seek = "Adjusted net national income per capita (current US$)";
  String seperator = ",";

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    if (line == null | !!line.toString().isEmpty()) {
      logger.info("null found");
      return;
    }
    if (line.toString().contains(
        " Adjusted net national income per capita (current US$) ")) {
      String[] Split = line.toString().split(seperator);

      logger.info(" data splitted ");

      if (Split.length == lengthIndex) {

        String countryName = Split[countryIndex];
        try {
          double income = Double.parseDouble(Split[incomeIndex]);

          context.write(new Text(countryName), new DoubleWritable(income));
        } catch (NumberFormatException nfe) {
          logger.info("wrong format" + countryName);

          return;
        }

      }
    }
  }

}
