package wiki;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class keyMatrix_filecreation {
    public static class preprecessing2M
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] vertex = value.toString().split("\t");
            String pageName = vertex[0];

            if (vertex.length==1){
                String d_outlinks = "0";
                context.write(new Text("1"),new Text(pageName+":"+d_outlinks));
            }else{
                String outlinks = String.valueOf(vertex[1].length());
                context.write(new Text("1"),new Text(pageName+":"+outlinks));
            }

        }
    }
    public static class preprecessing2R
            extends Reducer<Text, Text, Text, Text> {
        double sizeOfgraph=0.0;
        private MultipleOutputs mos;
        public void setup(Reducer.Context context) {
            Counter sizeOfgraph1;
            Configuration conf = context.getConfiguration();
            sizeOfgraph = conf.getDouble("NumOfNodes", 0.0);
            sizeOfgraph1 = context.getCounter(MatrixPageRank.numOfNodes.Counter);
            mos = new MultipleOutputs<Text, Text>(context);
        }
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> temp = new HashSet<String>();
            int counter =0;
            int graphNodes=0;

            double RMatrix_value = 1/sizeOfgraph;
            String outpath1 ="RMatrix/RMatrixWithID";
            String outpath2="KeyFolder/keyFile";

            for(Text vals : values){
                String name_cj = vals.toString();
                String name = name_cj.split(":")[0];
                String cj = name_cj.split(":")[1];

                mos.write(new Text(name), new Text(counter+":"+cj),outpath2);
                mos.write(new Text(String.valueOf(counter)),new Text(name+":"+String.valueOf(RMatrix_value)),outpath1);
                counter = counter + 1;
            }
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }
}
