package wiki;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class getKey {


//    public class getKeyM extends Mapper<Object, Text, Text, Text> {
//    MapWritable keyMap = new MapWritable();
//    protected void setup(Context context) throws IOException, InterruptedException {
//        super.setup(context);
//        URI[] files = context.getCacheFiles(); // getCacheFiles returns null
//        Path file1path = new Path(files[0]);
//        File some_file = new File(file1path.toString());
//        BufferedReader bufferedReader = new BufferedReader(new FileReader(some_file));
//
//        String line ="";
//
//        while ((line = bufferedReader.readLine()) != null) {
//            String name = line.split("\t")[0].trim();
//            String key1_cj = line.split("\t")[1].trim();
////            String key1 = key1_cj.split(":")[0].trim();
//
//            keyMap.put(new Text (name),new Text(key1_cj));
//        }}
//
//
//    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//        String[] vertex = value.toString().split("\t");
//        String pageName = vertex[0].split(":")[0];
//        String cj = vertex[0].split(":")[1];
//        String pageId = keyMap.get(new Text(pageName)).toString();
//        if (vertex.length==1){
//
//            context.write(new Text(pageId),new Text("Dummy"));
//        }else{
//            String[] links = vertex[1].split(",");
//            StringBuilder builder = new StringBuilder();
//
//            for(int x = 0; x < links.length;x++){
//                String str = links[x].trim();
//                String id_outlink = keyMap.get(new Text(str)).toString();
//                context.write(new Text(id_outlink),new Text(pageId));
//
//
//            }
//
//        }
//    }
//}
//    public static class getKeyR
//            extends Reducer<Text, Text, Text, Text> {
//        public void reduce(Text key, Iterable<Text> values,
//                           Context context) throws IOException, InterruptedException {
//            Set<String> temp = new HashSet<String>();
//            StringBuilder builder = new StringBuilder();
////            if (key == new Text("Dummy")){
////                for (Text vals : values) {
////                    context.write(vals,new Text(""));
////                }
////
////
////            }else{
//            for (Text vals : values) {
//                builder.append(vals.toString()).append(",");
//            }
//            context.write(key,new Text(builder.toString()));
////            }
//
//        }
//    }
}
