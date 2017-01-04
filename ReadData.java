import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

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

public class ReadData{

   /*public String[] getColumnsInColumnFamily(Result r, String columnFamily) {
      NavigableMap<byte[], byte[]> familyMap = r.getFamilyMap(Bytes.toBytes(columnFamily));
      String[] quantifiers = new String[familyMap.size()];

      int counter = 0;
      for (byte[] bQuantifier : familyMap.keySet()) {
         quantifiers[counter++] = Bytes.toString(bQuantifier);
      }
      return quantifiers;
   }*/
   private static final String FILENAME = "index.html";

   public static void main(String[] args) throws IOException {
      
      BufferedWriter bw = null;
      FileWriter fw = null;

      String content = "<!DOCTYPE html><html><head>    <meta charset=\"utf-8\">    <title>Heatmaps</title>    <style>      html, body {        height: 100%;        margin: 0; padding: 0;      }      #map {        height: 100%;      }#floating-panel {  position: absolute;  top: 10px;  left: 25%;  z-index: 5;  background-color: #fff;  padding: 5px;  border: 1px solid #999;  text-align: center;  font-family: 'Roboto','sans-serif';  line-height: 30px;  padding-left: 10px;}      #floating-panel {        background-color: #fff;        border: 1px solid #999;        left: 25%;        padding: 5px;        position: absolute;        top: 10px;        z-index: 5;      }    </style>  </head>  <body>    <div id=\"map\"></div>    <script>var map, heatmap;\nfunction initMap() {  map = new google.maps.Map(document.getElementById('map'), {    zoom: 13,    center: {";

      



      //Long rowid =  Long.parseLong(args[0], 10);
      //System.out.println(rowid);
      Configuration config = HBaseConfiguration.create();

      // Instantiating HTable class
      HTable hTable = new HTable(config, "summary_knn");

      Scan scan = new Scan();
      //scan.setCaching(20);
      //scan.setReversed(true);
      //scan.setMaxResultSize(1);

      scan.addFamily(Bytes.toBytes("results"));
      ResultScanner rs = hTable.getScanner(scan);

      Result result = rs.next();
      Result last_result;
      Boolean flag = false;
      while (result != null) {
         last_result = result;
         result = rs.next();

         if(result==null) {
            result = last_result;
            HTable hTable2 = new HTable(config, "geo3");
            //Scan scan2 = new Scan();
            //scan.addFamily(Bytes.toBytes("geo"));
            //scan.addFamily(Bytes.toBytes("results"));

            for(KeyValue keyValue : result.list()) {
               //System.out.println("Row: " + result.getRow() + " Qualifier : " + Bytes.toString(keyValue.getQualifier()) + " : Value : " + Bytes.toString(keyValue.getValue()));
               String str_id_dist = Bytes.toString(keyValue.getValue());
               String[] id_dist_arr = str_id_dist.split(",");
               //System.out.println("ID: " + id_dist_arr[0] + " Dist: " + id_dist_arr[1]);

               Get get = new Get(Bytes.toBytes(id_dist_arr[0]));
               //get.addFamily(Bytes.toBytes("geo"));
               get.addFamily(Bytes.toBytes("results"));
               Result user_res = hTable2.get(get);

               for(KeyValue user_keyValue : user_res.list()) {
                  String q = Bytes.toString(user_keyValue.getQualifier());
                  
                  String latlng = Bytes.toString(user_keyValue.getValue());

                  String str_latlng = Bytes.toString(user_keyValue.getValue());
                  String[] latlng_arr = str_latlng.split(",");
                  if (!flag) {
                     content += "lat: "+latlng_arr[0]+", lng: "+latlng_arr[1];
                     content += "},    mapTypeId: google.maps.MapTypeId.ROADMAP  });  heatmap = new google.maps.visualization.HeatmapLayer({    data: getPoints(),    map: map  });} function toggleHeatmap() {  heatmap.setMap(heatmap.getMap() ? null : map);} function changeGradient() {  var gradient = [    'rgba(0, 255, 255, 0)',    'rgba(0, 255, 255, 1)',    'rgba(0, 191, 255, 1)',    'rgba(0, 127, 255, 1)',    'rgba(0, 63, 255, 1)',    'rgba(0, 0, 255, 1)',    'rgba(0, 0, 223, 1)',    'rgba(0, 0, 191, 1)',    'rgba(0, 0, 159, 1)',    'rgba(0, 0, 127, 1)',    'rgba(63, 0, 91, 1)',    'rgba(127, 0, 63, 1)',    'rgba(191, 0, 31, 1)',    'rgba(255, 0, 0, 1)'  ];  heatmap.set('gradient', heatmap.get('gradient') ? null : gradient);} function changeRadius() {  heatmap.set('radius', heatmap.get('radius') ? null : 20);} function changeOpacity() {  heatmap.set('opacity', heatmap.get('opacity') ? null : 0.2);} \n function getPoints() {  return [            ";
                     flag = true;
                  }
                  if(q.equals("midpoint")) {
                     content += "new google.maps.LatLng("+latlng_arr[0]+","+latlng_arr[1]+" ),";
                     System.out.println("ID: " + id_dist_arr[0] + " Lat: " + latlng_arr[0] + " Lng: " + latlng_arr[1]);
                  }
                  

               }


            }
            break;
         }

         
      }
      
      // closing HTable
      hTable.close();



      content += "];}\n    </script>    <script async defer        src=\"https:/maps.googleapis.com/maps/api/js?key=AIzaSyAn-kSkQIqYOp0hyzdOwjr3ULpbJqV3KCE&signed_in=true&libraries=visualization&callback=initMap\"></script>  </body></html>";

      try {

         fw = new FileWriter(FILENAME);
         bw = new BufferedWriter(fw);
         bw.write(content);

         System.out.println("Done");

      } catch (IOException e) {

         e.printStackTrace();

      } finally {

         try {

            if (bw != null)
               bw.close();

            if (fw != null)
               fw.close();

         } catch (IOException ex) {

            ex.printStackTrace();

         }

      }

   }
}