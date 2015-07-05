package cloudwick.samples.dist_cache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinDist_cacheMapper extends Mapper<Object, Text, Text, Text> {

  Path[] cachefiles = new Path[0]; // To store the path of lookup files
  List<String> Artists = new ArrayList<String>();// To store the data of lookup

  // files

  @Override
  public void setup(Context context)

  {
    Configuration conf = context.getConfiguration();

    try {

      cachefiles = DistributedCache.getLocalCacheFiles(conf);


      BufferedReader reader1 =
          new BufferedReader(new FileReader(cachefiles[0].toString()));

      String line1;

      while ((line1 = reader1.readLine()) != null) {

        // Data of lookup files get stored in list object
        Artists.add(line1);

      }

    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  private final int StateIndex = 3;
  String seek = "night";
  String seperator = "<SEP>";

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    String[] splits = line.toString().split(seperator);

    if (splits.length == StateIndex + 1) {

      String Trackid = splits[StateIndex - 3];
      String Artistname = splits[StateIndex - 1];
      String Title = splits[StateIndex];

      for (String e : Artists) {

        String[] listLine = e.toString().split(seperator);

        if (listLine.length > 0) {
          String Track = listLine[2];

          if (Trackid.equals(Track)) {
            context.write(new Text(Trackid), new Text(listLine[0] + "\t"
                + Artistname + "\t" + Title));

          }
        }
      }
    }
  }
}
