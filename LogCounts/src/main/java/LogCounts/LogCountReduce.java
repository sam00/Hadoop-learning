//package LogCounts;
//
//import java.io.IOException;
//
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer;
//import LogCounts.Counter_enum;
//
//public class LogCountReduce extends
//    Reducer<Text, IntWritable, Text, IntWritable> {
//
//  public void reduce(Text key, Iterable<IntWritable> values, Context context)
//      throws IOException, InterruptedException {
//
//    int maxValue = Integer.MIN_VALUE;
//    for (IntWritable value : values) {
//      maxValue = Math.max(maxValue, value.get());
//
//    }
//
//    for (IntWritable value : values) {
//
//      context.getCounter(Counter_enum.RECORDS).increment(1);
//
//    context.write(key, new IntWritable(maxValue));
//  }
//}
//
// }