package SecondarySortTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import ProjectCounter.Compositekeywrite;
import ProjectCounter.CountryIncomeReducer;
import ProjectCounter.ImpCounterMap;

public class CountryIncomeReducerTest {

  MapDriver<Object, Text, Compositekeywrite, NullWritable> mapDriver;
  ReduceDriver<Compositekeywrite, NullWritable, Compositekeywrite, NullWritable> reduceDriver;
  MapReduceDriver<Object, Text, Compositekeywrite, NullWritable, Compositekeywrite, NullWritable> mapReduceDriver;

  @Before
  public void setUp() {
    ImpCounterMap mapper = new ImpCounterMap();
    CountryIncomeReducer reducer = new CountryIncomeReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }


  @Test
  public void testReducer_ValidLine_ValidMap() throws IOException {

    // Arrange
    Compositekeywrite k = new Compositekeywrite("Norway", 65636.7724056934);
    List<NullWritable> values = new ArrayList<NullWritable>();
    values.add(NullWritable.get());

    reduceDriver.withInput(k, values);
    reduceDriver.withOutput(k, NullWritable.get());

    // Act and Assert
    reduceDriver.runTest();
  }

  @Test
  public void testMapper_InvalidLine_Null() throws IOException {
    mapDriver.withInput(new LongWritable(), new Text(
        "Nothing interesting in here!!!"));
    mapDriver.runTest();
  }
}

