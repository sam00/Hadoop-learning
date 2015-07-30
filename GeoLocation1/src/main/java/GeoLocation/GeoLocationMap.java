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


  private Logger logger = Logger.getLogger("Filter");

  Path[] Distribute = new Path[0];

  private final int length = 4;
  private final int Ipadd = 3;
  String sep = "\t";
  DatabaseReader Fileread;

  @Override
  public void setup(Context context)

  {
    Configuration conf = context.getConfiguration();

    try {

      Distribute = DistributedCache.getLocalCacheFiles(conf);

      File dataset = new File(Distribute[0].toString()); //

      Fileread = new DatabaseReader.Builder(dataset).build();

    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    if (line == null | !!line.toString().isEmpty()) {
      logger.info("null");
      context.getCounter(Counter_enum.NULL_OR_EMPTY).increment(1);
      return;
    }

    // Splitting the record with a space and removing eveything except

    String[] Splits = line.toString().toLowerCase().split(sep);

    if (Splits.length == length) {

      String url = Splits[Ipadd];
      if (!url.startsWith("http") && !url.startsWith("https")) {
        url = "http://" + url;
      }
      URL Url = new URL(url);
      String HostUrl = Url.getHost();

      InetAddress address = null;
      try {
        address = InetAddress.getByName(HostUrl);
      } catch (UnknownHostException e) {
        return;
      }

      InetAddress Address = InetAddress.getByName(address.getHostAddress());
      CityResponse result = null;
      try {
        result = Fileread.city(Address);
      } catch (GeoIp2Exception ex) {
        ex.printStackTrace();
        return;
      }

      Country countryName = result.getCountry();
      String count = countryName.getName();

      if (countryName.getName() == null) {
        return;
      }


      logger.info(result.getCity() + ", " + countryName.getName() + ", "
          + countryName.getIsoCode());
      IntWritable alpha = new IntWritable(1);

      context.write(new Text(count), alpha);

    } else
      context.getCounter(Counter_enum.MISSING_FIELDS_RECORD_COUNT).increment(1);

  }
}