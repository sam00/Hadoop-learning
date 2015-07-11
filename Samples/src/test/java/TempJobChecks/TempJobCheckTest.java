package TempJobChecks;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TempJobCheckTest {
  MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
  ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

  @Before
  public void setUp() {
    TempMap mapper = new TempMap();
    TempReduce reducer = new TempReduce();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }

  @Test
  public void testMapper() throws IOException {
    mapDriver.withInput(new LongWritable(), new Text(
        "  1853   1    8.4     2.7       4    62.8     ---"));
    mapDriver.withOutput(new Text("1853"), new DoubleWritable(8.4));
    mapDriver.runTest();
  }

  // @Test
  // public void testReducer() {
  // List<IntWritable> values = new ArrayList<IntWritable>();
  // values.add(new IntWritable(1));
  // values.add(new IntWritable(1));
  // reduceDriver.withInput(new Text("6"), values);
  // reduceDriver.withOutput(new Text("6"), new DoubleWritable(2));
  // reduceDriver.runTest();
  // }
  //
  // @Test
  // public void testMapReduce() {
  // mapReduceDriver.withInput(new LongWritable(), new Text(
  // "655209;1;796764372490213;804422938115889;6"));
  // List<IntWritable> values = new ArrayList<IntWritable>();
  // values.add(new IntWritable(1));
  // values.add(new IntWritable(1));
  // mapReduceDriver.withOutput(new Text("6"), new DoubleWritable(2));
  // mapReduceDriver.runTest();
}


