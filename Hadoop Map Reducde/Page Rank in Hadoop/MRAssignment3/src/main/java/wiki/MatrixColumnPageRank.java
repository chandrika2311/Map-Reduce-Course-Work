package wiki;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.hash.Hash;
import org.fusesource.leveldbjni.All;

import java.io.*;
import java.net.URI;

public class MatrixColumnPageRank {
        public enum numOfNodes{
            Counter;
        }
        public static class getKeyM
                extends Mapper<Object, Text, Text, Text> {
            MapWritable keyMap = new MapWritable();
            private MultipleOutputs mos;
            double sizeOfgraph=0.0;
            Counter sizeOfgraph1;
            public void setup(Context context) throws IOException {
                try {
                    super.setup(context);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Counter sizeOfgraph1;
                mos = new MultipleOutputs<Text, Text>(context);
                Configuration conf = context.getConfiguration();
                sizeOfgraph = conf.getDouble("NumOfNodes", 0.0);
                sizeOfgraph1 = context.getCounter(wiki.MatrixColumnPageRank.numOfNodes.Counter);
                URI[] files = new URI[0]; // getCacheFiles returns null
                try {
                    files = context.getCacheFiles();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                for (int i = 0; i<files.length;i++){


                    Path file1path = new Path(files[0]);
                    BufferedReader bufferedReader = null;
                    try {
                        bufferedReader = new BufferedReader(new FileReader(new File(file1path.getName())));
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                    String line ="";
                    while ((line = bufferedReader.readLine()) != null) {
                        String name = line.split("\t")[0].trim();
                        String key1_cj = line.split("\t")[1].trim();
                        keyMap.put(new Text (name),new Text(key1_cj));
                    }}
            }


            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                String[] vertex = value.toString().split("\t");
                String pageName = vertex[0].split(":")[0];
                String cj = vertex[0].split(":")[1];
                String pageId = keyMap.get(new Text(pageName)).toString();
                if (vertex.length==1){
                    context.write(new Text(pageId),new Text("Dummy"));
                }else{
                    String[] links = vertex[1].split(",");
                    StringBuilder builder = new StringBuilder();

                    for(int x = 0; x < links.length;x++){
                        String str = links[x].trim();
                        String id_outlink = keyMap.get(new Text(str)).toString();
                        context.write(new Text(id_outlink),new Text(pageId));
                    } }}}
        public static class getKeyR
                extends Reducer<Text, Text, Text, Text> {
            public void reduce(Text key, Iterable<Text> values,
                               Context context) throws IOException, InterruptedException {
                Set<String> temp = new HashSet<String>();
                StringBuilder builder = new StringBuilder();
                for (Text vals : values) {
                    builder.append(vals.toString()).append(",");
                }
                context.write(key,new Text(builder.toString()));
            }
        }

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

    public static class finalRankingReducer extends Reducer<Text, Text, Text, Text>{


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
        //----------------------Main---------------------------------------------------------------------------------
        //----------------------Main---------------------------------------------------------------------------------
        //----------------------Main---------------------------------------------------------------------------------
        public static void main(String[] args) throws Exception {
            Configuration conf1 = new Configuration();
            double sizeOfgraph=0.0;
            conf1.setDouble("NumOfNodes",sizeOfgraph);
            Job job1 = Job.getInstance(conf1, "Preprocessing");
            job1.setJarByClass(preprocessingWithParser.class);
            job1.setMapperClass(preprocessingWithParser.preprecessingMapper.class);
            job1.setReducerClass(preprocessingWithParser.preprecessingReducer.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path(args[1]));
            job1.waitForCompletion(true);
            sizeOfgraph = job1.getCounters().findCounter(wiki.MatrixColumnPageRank.numOfNodes.Counter).getValue();
//---------------------------------------------------------------------------------------------------------------------
            Configuration conf2 = new Configuration();
            conf2.setDouble("NumOfNodes",sizeOfgraph);

            Job job2 = Job.getInstance(conf2, "keyMatrix_filecreation");
            job2.setJarByClass(keyMatrix_filecreation.class);
            job2.setMapperClass(keyMatrix_filecreation.preprecessing2M.class);
            job2.setReducerClass(keyMatrix_filecreation.preprecessing2R.class);
            job2.setOutputKeyClass(Text.class);
            job2.setNumReduceTasks(1);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[1]+"keysFile"));
            LazyOutputFormat.setOutputFormatClass(job2, TextOutputFormat.class);
            MultipleOutputs.addNamedOutput(job2, "keyValues", TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job2, "matrixValues", TextOutputFormat.class, Text.class, Text.class);
            job2.waitForCompletion(true);
//
//---------------------------------------------------------------------------------------------------------------------
            Configuration conf3 = new Configuration();
            Job job3 = Job.getInstance(conf3, "Getkeys");
            String inputPath=args[1]+"keysFile";
            Path cacheFiles = new Path(inputPath, "KeyFolder");
            FileSystem fs = cacheFiles.getFileSystem(conf3);
            FileStatus[] fileStatus = fs.listStatus(cacheFiles);
            for (FileStatus status : fileStatus) {
                job3.addCacheFile(status.getPath().toUri());
            }
//        job3.addCacheFile(new URI(args[1]+"keysFile"+"/KeyFolder"));
            job3.setJarByClass(wiki.MatrixColumnPageRank.class);
            job3.setMapperClass(wiki.MatrixColumnPageRank.getKeyM.class);
            job3.setReducerClass(wiki.MatrixColumnPageRank.getKeyR.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job3, new Path(args[1]));
            FileOutputFormat.setOutputPath(job3, new Path(args[1]+"newMatrix"));
            job3.waitForCompletion(true);
////---------------------------------------------------------------------------------------------------------------------

            for(int i = 0; i<10; i++) {
                Configuration conf4 = new Configuration();
                conf4.setDouble("NumOfNodes",sizeOfgraph);
                Job job4 = Job.getInstance(conf4, "doRanking");

                if(i==0) {
                    String inputPath2=args[1]+"keysFile";
                    Path cacheFiles2 = new Path(inputPath2, "RMatrix");
                    FileSystem fs2 = cacheFiles2.getFileSystem(conf4);
                    FileStatus[] fileStatus2 = fs2.listStatus(cacheFiles2);
                    for (FileStatus status : fileStatus2) {
                        job4.addCacheFile(status.getPath().toUri());
                    }
                }else{
//
                    String inputPath2=args[1]+"Rmatrix"+(i-1);
                    Path cacheFiles2 = new Path(inputPath2);
                    FileSystem fs2 = cacheFiles2.getFileSystem(conf4);
                    FileStatus[] fileStatus2 = fs2.listStatus(cacheFiles2);
                    for (FileStatus status : fileStatus2) {
                        job4.addCacheFile(status.getPath().toUri());
                    }}

                job4.setJarByClass(wiki.colDoRanking.class);
                job4.setMapperClass(wiki.colDoRanking.doRankingMap.class);
                job4.setReducerClass(wiki.colDoRanking.doRankingRed.class);
                job4.setOutputKeyClass(Text.class);
                job4.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job4, new Path(args[1] + "newMatrix"));
                FileOutputFormat.setOutputPath(job4, new Path(args[1] + "tempValues"+i));
//            +i
                job4.waitForCompletion(true);


                Configuration conf6 = new Configuration();
                conf6.setDouble("NumOfNodes",sizeOfgraph);
                Job job6 = Job.getInstance(conf6, "finalRanking");
                job6.setJarByClass(wiki.colFinalRanking.class);
                job6.setMapperClass(wiki.colFinalRanking.finalRankingMapper.class);
                job6.setReducerClass(wiki.colFinalRanking.finalRankingReducer.class);
                job6.setOutputKeyClass(Text.class);
                job6.setOutputValueClass(Text.class);
                //                File Cache
                if(i==0){
                    String inputPath3=args[1]+"keysFile";
                    Path cacheFiles3 = new Path(inputPath3, "RMatrix");
                    FileSystem fs3 = cacheFiles3.getFileSystem(conf6);
                    FileStatus[] fileStatus3 = fs3.listStatus(cacheFiles3);
                    for (FileStatus status : fileStatus3) {
                        job6.addCacheFile(status.getPath().toUri());
                    }

                }else{

                    String inputPath3=args[1]+"Rmatrix"+(i-1);
                    Path cacheFiles3 = new Path(inputPath3);
                    FileSystem fs3 = cacheFiles3.getFileSystem(conf6);
                    FileStatus[] fileStatus3 = fs3.listStatus(cacheFiles3);
                    for (FileStatus status : fileStatus3) {
                        job6.addCacheFile(status.getPath().toUri());
                    }
                }

                //                File Cache End
                FileInputFormat.addInputPath(job6, new Path(args[1] + "tempValues"+i));
//                +i
                FileOutputFormat.setOutputPath(job6, new Path(args[1] + "Rmatrix"+i));
//            +i
                job6.waitForCompletion(true);


//


            }
//---------------------------------------------------------------------------------------------------------------------
            Configuration conf5 = new Configuration();
            Job job5 = Job.getInstance(conf5, "Getkeys");
            job5.setJarByClass(matrixTop100.class);
            job5.setMapperClass(matrixTop100.top100pageRanksMapper.class);
            job5.setReducerClass(matrixTop100.top100pageRanksReducer.class);
            job5.setOutputKeyClass(DoubleWritable.class);
            job5.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job5, new Path(args[1]+"Rmatrix9"));
            FileOutputFormat.setOutputPath(job5, new Path(args[1]+"Top100"));
            job5.waitForCompletion(true);
        }}



///user/chandrikasharma//user/chandrikasharma/keys
//URI uri = new URI("localhost:50070/explorer.html#/user/chandrikasharma/keys");

