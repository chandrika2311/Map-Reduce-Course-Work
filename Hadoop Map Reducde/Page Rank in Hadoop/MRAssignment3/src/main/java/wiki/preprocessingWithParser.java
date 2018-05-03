package wiki;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class preprocessingWithParser {

    public static class preprecessingMapper
            extends Mapper<Object, Text, Text, Text> {

        private static Pattern namePattern;
        public List<String> allLinks;
        private static Pattern linkPattern;

        //---------------Setup function
        public void setup(Context context) {
            // Keep only html pages not containing tilde (~).
            namePattern = Pattern.compile("^([^~]+)$");
            // Keep only html filenames ending relative paths and not containing tilde (~).
            linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
        }
        List adjList = new ArrayList<String>();



        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                Bz2WikiParser1 bz2 = new Bz2WikiParser1();
                String x = bz2.parsing(String.valueOf(value));

//                System.out.println(x);
                String[] vertex = x.split("\t");
                if(vertex.length >1) {
                    String pageName = vertex[0];
                    String[] links = vertex[1].split(",");
//                    Emit the actual data
                    context.write(new Text(pageName), new Text (vertex[1]));
                    for(int i = 0; i<links.length; i++){
                        context.write(new Text(links[i].trim()), new Text("Dummy"));
                    }
                }else{
                    String pageName = vertex[0];
                    context.write(new Text(pageName), new Text ("Dummy"));
                }
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            } catch (SAXException e) {
                e.printStackTrace();
            }
        }

    }
    public static class preprecessingReducer
            extends Reducer<Text, Text, Text, Text> {
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            double sizeOfgraph=0.0;
            Configuration conf = context.getConfiguration();
            sizeOfgraph = conf.getDouble("NumOfNodes",0.0);
            context.getCounter(MatrixPageRank.numOfNodes.Counter).setValue(0);
        }
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            Set<String> temp = new HashSet<String>();

            for(Text vals : values){
                String[] x = (vals.toString().split(","));
                for(int i = 0; i<x.length; i++){
                    temp.add(x[i]);
                }
            }

            temp.remove("Dummy");

            StringBuilder builder = new StringBuilder();
            for (String str : temp) {
                builder.append(str).append(",");
            }
            String outlinks =  String.valueOf(temp.size());
            Text newValues =new Text(builder.toString());
            context.getCounter(MatrixPageRank.numOfNodes.Counter).increment((long) 1);
            context.write(new Text(key+":"+outlinks),newValues);

        }}
}
