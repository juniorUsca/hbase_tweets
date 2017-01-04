import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Geo1{

   public static void main(String[] args) throws IOException {

      Configuration config = HBaseConfiguration.create();
      HTable hTable = new HTable(config, "geo3");
      hTable.setAutoFlush(false);
      hTable.setWriteBufferSize(1024*1024*256);

      int gcont=30;
      long numRecords = 0;

      while(gcont < 50) {

         try {
            BufferedReader buf = new BufferedReader(new FileReader("loc-gowalla_totalCheckins.txt"));
            ArrayList<String> words = new ArrayList<>();
            String lineJustFetched = null;
            String[] wordsArray;
            int i = 0;
            int cont = 0;

            String id = String.valueOf(gcont)+"gow";
            String current_id = "";

            String row_id = "";
            String row_date = "";

            String row_lat = "";
            String row_lng = "";

            Put p = null;


            lineJustFetched = buf.readLine();
            if (lineJustFetched == null) {
            } else {
               wordsArray = lineJustFetched.split("\t");
               //row_id = id + String.valueOf();
               row_id = id + wordsArray[0];
               current_id = row_id;
               p = new Put(Bytes.toBytes(row_id));
            }

            while (true) {
               lineJustFetched = buf.readLine();
               if (lineJustFetched == null) {
                  hTable.put(p);
                  System.out.println("File finished!!");
                  break;
               } else {
                  wordsArray = lineJustFetched.split("\t");
                  
                  row_id = id + wordsArray[0];
                  if ( !row_id.equals(current_id) ) {

                     hTable.put(p);

                     p = new Put(Bytes.toBytes(row_id));
                     cont = 0;

                     numRecords++;
                     if ((numRecords % 10000) == 0)
                        System.out.println("processed " + numRecords + " records so far");
                  }

                  //p.add(Bytes.toBytes("geo"), Bytes.toBytes( "lat" + String.valueOf(cont) ), Bytes.toBytes( wordsArray[2] ));
                  //p.add(Bytes.toBytes("geo"), Bytes.toBytes( "lng" + String.valueOf(cont) ), Bytes.toBytes( wordsArray[3] ));
                  //if (!wordsArray[2].equals("0.0")) {
                     p.add(Bytes.toBytes("geo"), Bytes.toBytes( "latlng" + String.valueOf(cont) ), Bytes.toBytes( wordsArray[2]+","+wordsArray[3] ));
                     p.add(Bytes.toBytes("extras"), Bytes.toBytes( "date" + String.valueOf(cont) ), Bytes.toBytes( wordsArray[1] ));
                  //}
                  
                  cont++;
               }
               i++;
            }

            buf.close();
         } catch(Exception e) {
            e.printStackTrace();
         }

         gcont++;
         System.out.println(gcont + " global count");
         hTable.flushCommits();
         System.out.println(gcont*20 + "mb data");

      }

      hTable.flushCommits();
      hTable.close();
      System.out.println("done");


      /*Configuration config = HBaseConfiguration.create();

      // Instantiating HTable class
      HTable hTable = new HTable(config, "geo1");

      // Instantiating Put class
      // accepts a row name.
      Put p = new Put(Bytes.toBytes("row1")); 

      p.add(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes("raju"));

      p.add(Bytes.toBytes("personal"),
      Bytes.toBytes("city"),Bytes.toBytes("hyderabad"));

      p.add(Bytes.toBytes("professional"),Bytes.toBytes("designation"),
      Bytes.toBytes("manager"));

      p.add(Bytes.toBytes("professional"),Bytes.toBytes("salary"),
      Bytes.toBytes("50000"));
      
      // Saving the put Instance to the HTable.
      hTable.put(p);
      
      // closing HTable
      hTable.close();*/
   }
}
