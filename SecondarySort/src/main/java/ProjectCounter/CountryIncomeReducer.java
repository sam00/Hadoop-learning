package ProjectCounter;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class CountryIncomeReducer
    extends
    Reducer<Compositekeywrite, NullWritable, Compositekeywrite, NullWritable> {

  private Logger logger = Logger.getLogger("FilterMapper");

  @Override
  public void reduce(Compositekeywrite key, Iterable<NullWritable> values,
      Context context) throws IOException, InterruptedException {

    long count = context.getCounter(Counter_enum.RECORDS).getValue();

    if (count == 30) {
      return; // Displaying only top 10 and lowest 10 countries
    }

    for (NullWritable value : values) {

      context.getCounter(Counter_enum.RECORDS).increment(1);

      context.write(key, NullWritable.get());

}

}
}