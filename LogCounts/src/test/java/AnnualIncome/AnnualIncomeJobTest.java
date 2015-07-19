//package AnnualIncome;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mrunit.mapreduce.MapDriver;
//import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
//import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
//import org.junit.Before;
//import org.junit.Test;
//
//import TempJobChecks.TempMap;
//import TempJobChecks.TempReduce;
//
//public class AnnualIncomeJobTest {
//
//  MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
//  ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
//  MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
//
//  @Before
//  public void setUp() {
//    TempMap mapper = new TempMap();
//    TempReduce reducer = new TempReduce();
//    mapDriver = MapDriver.newMapDriver(mapper);
//    reduceDriver = ReduceDriver.newReduceDriver(reducer);
//    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
//  }
//
//  @Test
//  public void testMapper_ValidLine_ValidMap() throws IOException {
//    mapDriver
//        .withInput(
//            new LongWritable(),
//            new Text(
//                " Afghanistan AFG Adjusted net national income per capita (current US$) NY.ADJ.NNTY.PC.CD                     1.51E+02  1.53E+02  1.30E+02  1.35E+02  1.63E+02  1.74E+02  1.85E+02  2.10E+02  2.33E+02  2.60E+02  2.50E+02  2.40E+02                                        1.02E+02  1.64E+02  1.76E+02  1.98E+02  2.29E+02  2.49E+02  3.37E+02  3.37E+02  4.06E+02  5.01E+02  5.50E+02  6.18E+02  5.99E+02  "));
//    mapDriver.withOutput(new Text("Afghanistan"), new DoubleWritable(
//        500.880766813369));
//    mapDriver.runTest();
//  }
//
//  @Test
//  public void testMapper_LineWithSpaces_ValidMap() throws IOException {
//    mapDriver.withInput(new LongWritable(), new Text(
//                " Afghanistan,AFG,Energy intensity level of primary energy (MJ/$2011 PPP GDP),EG.EGY.PRIM.PP.KD,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,1.43160381792921E+00,1.42520639968577E+00,1.30430954726099E+00,1.42501888763458E+00,1.56456724183161E+00,1.62080804853902E+00,2.31300583906042E+00,2.76864894567505E+00,3.11662955091309E+00,3.98320279611036E+00,4.63303038532039E+00,,"));
//    mapDriver.withOutput(new Text("Afghanistan"), new DoubleWritable(8.4));
//    mapDriver.runTest();
//  }
//
//  @Test
//  public void testMapper_LineWithStar_Null() throws IOException {
//    mapDriver.withInput(new LongWritable(), new Text(
//                " Afghanistan,AFG,Energy intensity level of primary energy (MJ/$2011 PPP GDP),EG.EGY.PRIM.PP.KD,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,1.43160381792921E+00,1.42520639968577E+00,1.30430954726099E+00,1.42501888763458E+00,1.56456724183161E+00,1.62080804853902E+00,2.31300583906042E+00,2.76864894567505E+00,3.11662955091309E+00,3.98320279611036E+00,4.63303038532039E+00,, "));
//    mapDriver.runTest();
//  }
//
//  @Test
//  public void testMapper_InvalidLine_Null() throws IOException {
//    mapDriver.withInput(new LongWritable(), new Text(
//        "Nothing interesting in here!!!"));
//    mapDriver.runTest();
//  }
//
//  @Test
//  public void testReducer_ValidMaps_ValidResult() throws IOException {
//
//    List<DoubleWritable> values = new ArrayList<DoubleWritable>();
//    double expected = 200.05;
//
//    values.add(new DoubleWritable(8.4));
//    values.add(new DoubleWritable(1.6));
//    values.add(new DoubleWritable(100));
//    values.add(new DoubleWritable(expected));
//
//    reduceDriver.withInput(new Text("2000"), values);
//    reduceDriver.withOutput(new Text("2000"), new DoubleWritable(expected));
//
//    reduceDriver.runTest();
//  }
//
//  @Test
//  public void testMapReduce() throws IOException {
//    mapReduceDriver.withInput(new LongWritable(), new Text(
//                "  Afghanistan,AFG,Energy intensity level of primary energy (MJ/$2011 PPP GDP),EG.EGY.PRIM.PP.KD,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,1.43160381792921E+00,1.42520639968577E+00,1.30430954726099E+00,1.42501888763458E+00,1.56456724183161E+00,1.62080804853902E+00,2.31300583906042E+00,2.76864894567505E+00,3.11662955091309E+00,3.98320279611036E+00,4.63303038532039E+00,,"));
//    List<DoubleWritable> values = new ArrayList<DoubleWritable>();
//    values.add(new DoubleWritable(1));
//    values.add(new DoubleWritable(1));
//    mapReduceDriver.withOutput(new Text("1853"), new DoubleWritable(8.4));
//    mapReduceDriver.runTest();
//  }
//
// }
