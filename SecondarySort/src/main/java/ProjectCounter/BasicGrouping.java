package ProjectCounter;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

public class BasicGrouping extends WritableComparator {
  protected BasicGrouping() {
    super(Compositekeywrite.class, true);
  }

  private Logger logger = Logger.getLogger("FilterMapper");

  @Override
  public int compare(WritableComparable w1, WritableComparable w2) {
    Compositekeywrite alpha = (Compositekeywrite) w1;
    Compositekeywrite beta = (Compositekeywrite) w2;

    return alpha.getIncome().compareTo(beta.getIncome());
  }
}