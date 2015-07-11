package TempJobChecks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
  public void testMapper_ValidLine_ValidMap() throws IOException {
    mapDriver.withInput(new LongWritable(), new Text(
        "  1853   1    8.4     2.7       4    62.8     ---"));
    mapDriver.withOutput(new Text("1853"), new DoubleWritable(8.4));
    mapDriver.runTest();
  }

  @Test
  public void testMapper_LineWithSpaces_ValidMap() throws IOException {
    mapDriver.withInput(new LongWritable(), new Text(
        "    1853   1    8.4     2.7       4    62.8     ---    "));
    mapDriver.withOutput(new Text("1853"), new DoubleWritable(8.4));
    mapDriver.runTest();
  }

  @Test
  public void testMapper_LineWithStar_Null() throws IOException {
    mapDriver.withInput(new LongWritable(), new Text(
        "    1853   1    8.4*     2.7       4    62.8     ---    "));
    mapDriver.runTest();
  }

  @Test
  public void testMapper_InvalidLine_Null() throws IOException {
    mapDriver.withInput(new LongWritable(), new Text(
        "Nothing interesting in here!!!"));
    mapDriver.runTest();
  }

  @Test
  public void testReducer_ValidMaps_ValidResult() throws IOException {

    List<DoubleWritable> values = new ArrayList<DoubleWritable>();
    double expected = 200.05;

    values.add(new DoubleWritable(8.4));
    values.add(new DoubleWritable(1.6));
    values.add(new DoubleWritable(100));
    values.add(new DoubleWritable(expected));

    reduceDriver.withInput(new Text("2000"), values);
    reduceDriver.withOutput(new Text("2000"), new DoubleWritable(expected));

    reduceDriver.runTest();
  }

  @Test
  public void testMapReduce() throws IOException {
    mapReduceDriver.withInput(new LongWritable(), new Text(
        "  1853   1    8.4     2.7       4    62.8     ---"));
    List<DoubleWritable> values = new ArrayList<DoubleWritable>();
    values.add(new DoubleWritable(1));
    values.add(new DoubleWritable(1));
    mapReduceDriver.withOutput(new Text("1853"), new DoubleWritable(8.4));
    mapReduceDriver.runTest();
  }

}
