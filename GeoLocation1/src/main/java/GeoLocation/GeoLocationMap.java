package GeoLocation;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.Country;


@SuppressWarnings("deprecation")
public class GeoLocationMap extends Mapper<Object, Text, Text, IntWritable> {

  // Path cachefiles = new Path(args[1]);
  private Logger logger = Logger.getLogger("FilterMapper");

  Path[] cachefiles = new Path[0];

  private final int lenIndex = 4;
  private final int IpIndex = 3;
  String seperator = "\t";
  DatabaseReader reader;

  @SuppressWarnings("deprecation")
  @Override
  public void setup(Context context)

  {
    Configuration conf = context.getConfiguration();

    try {

      cachefiles = DistributedCache.getLocalCacheFiles(conf);

      File database = new File(cachefiles[0].toString()); //

      reader = new DatabaseReader.Builder(database).build();

    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    if (line == null | !!line.toString().isEmpty()) {
      logger.info("null found.");
      context.getCounter(Counter_enum.NULL_OR_EMPTY).increment(1);
      return;
    }

    // Splitting the record with a space and removing eveything except
    // [^0-9a-zA-Z ]
    String[] recordSplits = line.toString().toLowerCase().split(seperator);

    if (recordSplits.length == lenIndex) {

      String url = recordSplits[IpIndex];
      if (!url.startsWith("http") && !url.startsWith("https")) {
        url = "http://" + url;
      }
      URL netUrl = new URL(url);
      String host = netUrl.getHost();

      InetAddress address = null;
      try {
        address = InetAddress.getByName(host);
      } catch (UnknownHostException e) {
        return;
      }

      InetAddress ipAddress = InetAddress.getByName(address.getHostAddress());
      CityResponse response = null;
      try {
        response = reader.city(ipAddress);
      } catch (GeoIp2Exception ex) {
        ex.printStackTrace();
        return;
      }

      Country country = response.getCountry();
      String count = country.getName(); // 'US'

      if (country.getName() == null) {
        return;
      }

      logger.info(response.getCity() + ", " + country.getName() + ", "
          + country.getIsoCode());
      IntWritable ONE = new IntWritable(1);

      context.write(new Text(count), ONE);

    } else
      context.getCounter(Counter_enum.MISSING_FIELDS_RECORD_COUNT).increment(1);

  }
}