package wiki;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.TreeMap;

public class top100pageRank {
    public static class top100pageRanksMapper
            extends Mapper<Object, Text, DoubleWritable, Text> {
        private TreeMap<Double, Text> localTop100Records = new TreeMap<Double, Text>();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            double pageRank=  0.0;
            String value1 = String.valueOf(value);
            String[] WholeValue = value1.split("\t");

            Node node = new Node();
            String pageName = WholeValue[0];
            node.setNodeId(pageName);

            String[] pgRank = WholeValue[1].split(":");

            if(pgRank.length ==2){
                pageRank =  Double.parseDouble(pgRank[1]);
                localTop100Records.put(pageRank, new Text(pageName));
//             context.write(new LongWritable(),new Text(pageName));
            }
            if (localTop100Records.size() > 100) {
                localTop100Records.remove(localTop100Records.firstKey());
            }
        }
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            Set<Double> keys = localTop100Records.keySet();
            for(Double key1: keys){
                System.out.println("Value of "+key1+" is: "+localTop100Records.get(key1));
                context.write(new DoubleWritable(key1),new Text(localTop100Records.get(key1)));
            }
        }
    }
    public static class top100pageRanksReducer
            extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        private TreeMap<DoubleWritable, Text> globalTop100Records = new TreeMap<>();
        public void reduce(DoubleWritable key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {

            for(Text val:values){
                if (globalTop100Records.size() > 100) {
                    globalTop100Records.remove(globalTop100Records.firstKey());
                }
                globalTop100Records.put(key, new Text(val));
            }

            Set<DoubleWritable> keys = globalTop100Records.keySet();

            for(DoubleWritable key1: keys){
                context.write(key1,new Text(globalTop100Records.get(key)));
            }

        }

    }

}

