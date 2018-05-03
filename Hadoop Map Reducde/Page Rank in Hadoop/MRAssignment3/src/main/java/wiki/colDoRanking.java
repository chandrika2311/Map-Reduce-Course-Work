package wiki;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class colDoRanking {
    public static class doRankingMap
            extends Mapper<Object, Text, Text, Text> {
        HashMap<String,String> rMatrix = new HashMap<>();
        protected void setup(Mapper.Context context) throws IOException {
//            super.setup(context);
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
                    String colId = line.toString().split("\t")[0].trim();
                    String name_pagerank = line.toString().split("\t")[1].trim();
//                    String col_id_name_value = colId+","+name_pagerank;
                    String value = name_pagerank.split(":")[1];
//                    rMatrix.put(colId,col_id_name_value);
                    rMatrix.put(colId,value);
//                    try {
//                        context.write(new Text(colId),new Text(col_id_name_value));
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                }}
        }


        public void map(Object key, Text value, Context context) {
//          Loading entire record:
            String[] record = value.toString().split("\t");
            String row_cj = record[0];
            String[] linksId_cj = record[1].split(",");
            for(int i = 0;i<linksId_cj.length;i++){
//                1384:17,10006:8,7942:661,10432:12,
                if(linksId_cj[i].equals("Dummy")){
                    continue;
                }
                String colId = linksId_cj[i].split(":")[0];
                Double cj = Double.valueOf(linksId_cj[i].split(":")[1]);
                Text row_id = new Text(row_cj.split(":")[0]);
                if (rMatrix.containsKey(colId)){
                    Double col_value_Rmatric = Double.valueOf(rMatrix.get(colId));
//                multiplying alpha and Beta here where both are present. i.e since we have the Beta value for this column
                    Double colValue = cj*col_value_Rmatric;
                    Text row_col_id_value = new Text (row_id+","+colId+","+colValue.toString());
                    //Emiting (i,(i,j,cj))
                    try {
                        context.write(new Text(colId),row_col_id_value);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }

            }


        }
        public void cleanup(Context context){
//            for (Map.Entry<String, String> entry : rMatrix.entrySet()) {
//                String col_id = entry.getKey();
//                String  col_id_name_value = entry.getValue();
//
//                try {
//                    context.write(new Text (col_id), new Text(col_id_name_value));
//                } catch (IOException e) {
//                    e.printStackTrace();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//
//            }
        }

    }
    public static class doRankingRed extends Reducer<Text, Text, Text, Text> {
        HashMap<String,Double> alphaForRows = new HashMap<>();
        Double Beta=0.0;


        public void reduce(Text key, Iterable<Text> values,
                           Context context) {

//                Take each record and check if on split(",") it has 2 values or 3 values:
            Double alpha = 0.0;
            for(Text vals : values){
                String[] value = vals.toString().split(",");
                if (value.length == 2){
//       its from the RMatrix
                    Beta = Double.valueOf(value[1].split(":")[1]);

                }else{
//       its from the A matrix(i,j,alpha)
                    String row_id = String.valueOf(value[0]);
                    alpha = Double.valueOf(value[2]);
                    if(alphaForRows.containsKey(row_id)){
                        Double val = alphaForRows.get(row_id);
                        val = val+(alpha);
                        alphaForRows.put(row_id,val);

                    }else{
                        alphaForRows.put(row_id,alpha);
                    }

                }
            }
        }

        public void cleanup(Context context){
            for (Map.Entry<String, Double> entry : alphaForRows.entrySet()) {
                String row = entry.getKey();
                Double alpha = entry.getValue();
                Double temp = alpha;
                try {
                    context.write(new Text (row), new Text(Double.toString(temp)));
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }}
    }
}
