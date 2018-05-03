package wiki;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.net.URI;

public class colFinalRanking {

    public static class finalRankingMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context){
            String row = value.toString().split("\t")[0];
            String tempValue = value.toString().split("\t")[1];
            try {
                context.write(new Text(row), new Text(tempValue));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class finalRankingReducer extends Reducer<Text, Text, Text, Text> {


        MapWritable pageRankMap = new MapWritable();
        protected void setup(Reducer.Context context) throws IOException {
            URI[] files = new URI[0]; // getCacheFiles returns null
            try {
                files = context.getCacheFiles();
            } catch (IOException e) {
                e.printStackTrace();
            }
            for (int i = 0; i<files.length;i++){
                Path file1path = new Path(files[i]);
                BufferedReader bufferedReader = null;
                try {
                    bufferedReader = new BufferedReader(new FileReader(new File(file1path.getName())));
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                String line ="";

                while ((line = bufferedReader.readLine()) != null) {
                    String id = line.split("\t")[0].trim();
                    String name_pagerank = line.split("\t")[1].trim();

                    pageRankMap.put(new Text(id),new Text(name_pagerank));
                }}
        }
        public void reduce(Text key, Iterable<Text> values,
                           Context context){
            Double all_TempValues = 0.0;
            for(Text vals: values){
//                All the temp values of a row come here
                all_TempValues = all_TempValues+Double.valueOf(vals.toString());
            }
            if(pageRankMap.containsKey(key)){
                String pagename = pageRankMap.get(key).toString().split(":")[0];
                try {
                    context.write(key, new Text(pagename + ":" +all_TempValues.toString()));
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }
}
