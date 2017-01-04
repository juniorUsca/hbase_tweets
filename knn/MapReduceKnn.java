import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.File;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import java.lang.*;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.hbase.KeyValue;

import org.apache.hadoop.io.Writable;

/**
 * counts the number of userIDs
 * 
 * @author sujee ==at== sujee.net
 * 
 */
public class MapReduceKnn {

    /*public static class DoubleString implements WritableComparable<DoubleString>
    {
        private Double distance = 0.0;
        private String model = null;

        public void set(Double lhs, String rhs)
        {
            distance = lhs;
            model = rhs;
        }
        
        public Double getDistance()
        {
            return distance;
        }
        
        public String getModel()
        {
            return model;
        }
        
        @Override
        public void readFields(DataInput in) throws IOException
        {
            distance = in.readDouble();
            model = in.readUTF();
        }
        
        @Override
        public void write(DataOutput out) throws IOException
        {
            out.writeDouble(distance);
            out.writeUTF(model);
        }
        
        @Override
        public int compareTo(DoubleString o)
        {
            return (this.model).compareTo(o.model);
        }
    }*/


    static class Mapper1 extends TableMapper<ImmutableBytesWritable, Text> {

        private int numRecords = 0;
        private static final DoubleWritable one = new DoubleWritable(1);

        //private Double req_lat = 57.717396066700005;
        //private Double req_lng = 12.0272543167;

        //private Double req_lat = 40.7341933833;
        //private Double req_lng = -74.0041635333;

        private Double req_lat = 39.891077;
        private Double req_lng = -105.068532;

        private Double distance(Double lat1, Double lng1, Double lat2, Double lng2) {
            return Math.sqrt( (lat1-lat2)*(lat1-lat2) + (lng1-lng2)*(lng1-lng2) );
        }

        @Override
        public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
            // extract userKey from the compositeKey (userId + counter)
            //ImmutableBytesWritable userKey = new ImmutableBytesWritable(row.get(), 0, Bytes.SIZEOF_INT);

            // mean median midpoint
            //String str_latlng = Bytes.toString( values.getColumnLatest(Bytes.toBytes("results"), Bytes.toBytes("midpoint")).getValue() )
            String str_latlng = Bytes.toString( values.getValue(Bytes.toBytes("results"), Bytes.toBytes("midpoint")) );
            //Bytes.toString(keyValue.getValue())

            /*for(KeyValue keyValue : values.list()) {
                System.out.println("Row: " + Bytes.toString(values.getRow()) + " Qualifier : " + Bytes.toString(keyValue.getQualifier()) + " : Value : " + Bytes.toString(keyValue.getValue()));
            }*/

            String[] latlng_arr = str_latlng.split(",");
            Double tmp_lat = Double.parseDouble(latlng_arr[0]);
            Double tmp_lng = Double.parseDouble(latlng_arr[1]);

            Double dist = distance(tmp_lat, tmp_lng, req_lat, req_lng);

            //System.out.println(Bytes.toString(row.get()) + "\t" + tmp_lat + "\t" + tmp_lng);

            try {
                //context.write(row, new DoubleWritable(dist));
                //System.out.println(Bytes.toString(row.get()) + "\t" + tmp_lat + "\t" + tmp_lng + "\t" + dist);
                ImmutableBytesWritable k = new ImmutableBytesWritable(Bytes.toBytes(1));
                String vv = new String(Bytes.toString(row.get())+","+dist);
                //System.out.println(vv);
                Text v = new Text( vv );
                context.write(k, v);
            } catch (InterruptedException e) {
                System.out.println( "Error aqui:" );
                System.out.println( e.getMessage() );
                throw new IOException(e);
            }
            numRecords++;
            if ((numRecords % 10000) == 0) {
                context.setStatus("mapper processed " + numRecords + " records so far");
                System.out.println("mapper processed " + numRecords + " records so far");
            }
        }
    }

    public static class Reducer1 extends TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {

        private Integer k = 1000;

        TreeMap<Double, String> KnnMap = new TreeMap<Double, String>();

        long rowkey = System.currentTimeMillis();

        public void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            //System.out.println("Esty aki!!!!");
            for (Text val : values) {
                //System.out.println(val.toString());
                String str_user_dist = val.toString();
                String[] user_dist_arr = str_user_dist.split(",");
                
                Double tDist = Double.parseDouble(user_dist_arr[1]);
                String userKey = user_dist_arr[0];

                KnnMap.put(tDist, userKey);
                if (KnnMap.size() > k)
                    KnnMap.remove(KnnMap.lastKey());
            }

            int cont = 0;


            Put put = new Put(Bytes.toBytes(rowkey));

            for(Map.Entry<Double,String> entry : KnnMap.entrySet()) {
                String map_value = entry.getValue();
                Double map_key = entry.getKey();

                System.out.println(cont + "   " + map_value + " => " + map_key);
                cont++;

                put.add(Bytes.toBytes("results"), Bytes.toBytes("userdist"+String.valueOf(cont)), Bytes.toBytes(map_value+","+String.valueOf(map_key)));
            }

            System.out.println("rowid: " + rowkey);
            context.write(key, put);
        }
    }

    public static void main(String[] args) throws Exception {
        HBaseConfiguration conf = new HBaseConfiguration();
        Job job = new Job(conf, "MapReduceKnn");
        job.setJarByClass(MapReduceKnn.class);
        Scan scan = new Scan();
        //String columns = "details"; // comma seperated
        //scan.addColumns(columns);
        scan.addFamily(Bytes.toBytes("results"));
        //scan.setFilter(new KeyOnlyFilter());
        //scan.setFilter(new FirstKeyOnlyFilter());
        TableMapReduceUtil.initTableMapperJob("geo3", scan, Mapper1.class, ImmutableBytesWritable.class,
                Text.class, job);
        TableMapReduceUtil.initTableReducerJob("summary_knn", Reducer1.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}