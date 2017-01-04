import java.io.*;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.KeyValue;

public class MidAlgorithms{

  static Double midPoint_lat, midPoint_lng;

  public static String midPoint(Result result) {
    Double lat, lng, x, y, z, w=1.0, weight_total=0.0, x_total=0.0, y_total=0.0, z_total=0.0, hyp;
    for(KeyValue keyValue : result.list()) {
      //System.out.println("Row: " + Bytes.toString(result.getRow()) + " Qualifier : " + Bytes.toString(keyValue.getQualifier()) + " : Value : " + Bytes.toString(keyValue.getValue()));
      String latlng = Bytes.toString(keyValue.getValue());
      String[] latlng_arr = latlng.split(",");

      //Convert Lat1 and Lon1 from degrees to radians.
      lat = Double.parseDouble(latlng_arr[0])*(Math.PI)/180;
      lng = Double.parseDouble(latlng_arr[1])*(Math.PI)/180;
      //Convert lat/lon to Cartesian coordinates for first location.
      x = Math.cos(lng)*Math.cos(lat);               
      y = Math.sin(lng)*Math.cos(lat);               
      z = Math.sin(lat);

      //Compute weight (by time) for first location.
      weight_total += w;
      x_total += x*w;
      y_total += y*w;
      z_total += z*w;
    }

    x_total = x_total/weight_total;
    y_total = y_total/weight_total;
    z_total = z_total/weight_total;

    //Convert average x, y, z coordinate to latitude and longitude. Note that in Excel and possibly some other applications, the parameters need to be reversed in the atan2 function, for example, use atan2(X,Y) instead of atan2(Y,X).
    lng=Math.atan2(y_total, x_total);
    hyp=Math.sqrt((x_total*x_total)+(y_total*y_total));
    lat=Math.atan2(z_total, hyp);

    //Convert lat and lon to degrees.
    lat = lat*180/Math.PI;
    lng = lng*180/Math.PI;

    midPoint_lat = lat;
    midPoint_lng = lng;

    return String.valueOf(lat)+","+String.valueOf(lng);
  }

  public static String mean(Result result) {

    Double size=0.0, lat_total=0.0, lng_total=0.0;

    for(KeyValue keyValue : result.list()) {
      String latlng = Bytes.toString(keyValue.getValue());
      String[] latlng_arr = latlng.split(",");
      lat_total+=Double.parseDouble(latlng_arr[0]);
      lng_total+=Double.parseDouble(latlng_arr[1]);
      size+=1.;
    }

    lat_total=lat_total/size;
    lng_total=lng_total/size;

    return String.valueOf(lat_total)+","+String.valueOf(lng_total);
  }

  public static Double totalDistances(Double lat, Double lng, Result result) {
    Double res = 0.0;
    for(KeyValue keyValue : result.list()) {
      String latlng = Bytes.toString(keyValue.getValue());
      String[] latlng_arr = latlng.split(",");
      Double lat2=Double.parseDouble(latlng_arr[0]);
      Double lng2=Double.parseDouble(latlng_arr[1]);

      Double dist = Math.asin( Math.sin(lat)*Math.sin(lat2) + Math.cos(lat)*Math.cos(lat2)*Math.cos(lng2-lng) );
      res+=dist;
    }
    return res;
  }

  public static String median(Result result) {
    Double currentPoint_lat = midPoint_lat;
    Double currentPoint_lng = midPoint_lng;


    Double min_dist = totalDistances(currentPoint_lat, currentPoint_lng, result);

    for(KeyValue keyValue : result.list()) {
      String latlng = Bytes.toString(keyValue.getValue());
      String[] latlng_arr = latlng.split(",");
      Double lat = Double.parseDouble(latlng_arr[0]);
      Double lng = Double.parseDouble(latlng_arr[1]);
      Double distance = totalDistances(lat, lng, result);
      if (distance<min_dist) {
        currentPoint_lat = lat;
        currentPoint_lng = lng;
        min_dist = distance;
      }
    }

    return String.valueOf(currentPoint_lat)+","+String.valueOf(currentPoint_lng);
  }

  public static void main(String[] args) throws IOException {
    Configuration config = HBaseConfiguration.create();
    HTable hTable = new HTable(config, "geo3");
    hTable.setAutoFlush(false);
    hTable.setWriteBufferSize(1024*1024*256);

    Scan scan = new Scan();
    //scan.setCaching(20);
    scan.addFamily(Bytes.toBytes("geo"));
    ResultScanner rs = hTable.getScanner(scan);
    long c = 0;
    for (Result result = rs.next(); (result != null); result = rs.next()) {

      String midPoint_val = midPoint(result);
      String mean_val = mean(result);
      String median_val = median(result);

      //System.out.println(c+" midpoint: "+midPoint_val+" \tmean: "+mean_val+"\t median: "+median_val);
      if ((c % 10000) == 0)
        System.out.println("processed " + c + " records so far");

      Put p = new Put(result.getRow());
      p.add(Bytes.toBytes("results"), Bytes.toBytes( "midpoint" ), Bytes.toBytes( midPoint_val ));
      p.add(Bytes.toBytes("results"), Bytes.toBytes( "mean" ), Bytes.toBytes( mean_val ));
      p.add(Bytes.toBytes("results"), Bytes.toBytes( "median" ), Bytes.toBytes( mean_val ));
      hTable.put(p);

      c++;
    }
      
    // closing HTable
    hTable.flushCommits();
    hTable.close();
    System.out.println("Done!");

  }
}
