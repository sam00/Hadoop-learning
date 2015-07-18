package ProjectCounter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class BasicPartitioner extends
    Partitioner<Compositekeywrite, NullWritable> {

  @Override
  public int getPartition(Compositekeywrite key, NullWritable value,
      int numReduceTasks) {

    return (key.getCountryName().hashCode() % numReduceTasks);
}
}