import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Combiner {
    static Text max_type = new Text("TMAX");
    static Text max_count = new Text("TMAX_COUNT");
    static Text min_type = new Text("TMIN");
    static Text min_count = new Text("TMIN_COUNT");
    static Text max_average = new Text("MAX_AVERAGE");
    static Text min_average = new Text("MIN_AVERAGE");
    public static class CombinerMapper
            extends Mapper<Object, Text, Text, MapWritable> {
        //Tryingt o change the implementation to MapWritable
//        private final static IntWritable one = new IntWritable();
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            MapWritable StationDeets=new MapWritable();
            String station;
            String type;
            String Temp;
            int temp = 0;
            station = value.toString().split(",")[0];
            type = value.toString().split(",")[2];
            Temp = value.toString().split(",")[3];
            word.set(station);
            if (type.equals("TMAX") ){
                temp = Integer.parseInt(Temp);//Converting the maxTemp in string format to Integer format
                StationDeets.put(max_type,new IntWritable(temp));
                StationDeets.put(max_count,new IntWritable(1));
                context.write(word, new MapWritable(StationDeets));

            }

            else if (type.equals("TMIN") ) {
                temp = Integer.parseInt(Temp);//Converting the maxTemp in string format to Integer format

                StationDeets.put(min_type, new IntWritable(temp));
                StationDeets.put(min_count, new IntWritable(1));
                context.write(word, new MapWritable(StationDeets)); }
        }

    }
    public static class AvgStationTempCombiner
            extends Reducer<Text,MapWritable,Text,MapWritable> {
        MapWritable combinedStationDeets = new MapWritable();

        public void reduce(Text key, Iterable<MapWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            int tmax = 0;
            int tmin = 0;
            int tmax_count=0;
            int tmin_count=0;

            for (MapWritable val : values) {
                try{
                    tmax += ((IntWritable) val.get(max_type)).get();
                    tmax_count += ((IntWritable) val.get(max_count)).get();
                }catch(Exception e){
                    tmax += 0;
                    tmax_count+=0;}
                try{
                    tmin += ((IntWritable) val.get(min_type)).get();
                    tmin_count += ((IntWritable) val.get(min_count)).get();
                }catch(Exception e){
                    tmin +=0;
                    tmin_count+=0; }}

            combinedStationDeets.put(max_type,new IntWritable(tmax));
            combinedStationDeets.put(max_count,new IntWritable(tmax_count));
            combinedStationDeets.put(min_type,new IntWritable(tmin));
            combinedStationDeets.put(min_count,new IntWritable(tmin_count));
            context.write(key, combinedStationDeets);
        }
    }
    public static class AvgStationTempReducer
            extends Reducer<Text,MapWritable,Text,MapWritable> {
        MapWritable maxMinTemps = new MapWritable();

        public void reduce(Text key, Iterable<MapWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            System.out.println("INSIDE REDUCER");
            int tmax = 0;
            int tmin = 0;
            int tmax_count=0;
            int tmin_count=0;

            for (MapWritable val : values) {
                try{
                    tmax += ((IntWritable) val.get(max_type)).get();
                    tmax_count += ((IntWritable) val.get(max_count)).get();
                }catch(Exception e){
                    tmax += 0;
                    tmax_count+=0;}
                try{
                    tmin += ((IntWritable) val.get(min_type)).get();
                    tmin_count += ((IntWritable) val.get(min_count)).get();
                }catch(Exception e){
                    tmin +=0;
                    tmin_count+=0;
                }}
            float max_average_val;
            float min_average_val;
            try{
                max_average_val = tmax/(float)tmax_count;
            }catch(Exception e){

                max_average_val=0;
            }
            try{

                min_average_val = tmin/(float)tmin_count;
            }catch(Exception e){
                min_average_val=0;
            }
            maxMinTemps.put(max_average,new Text(String.valueOf(max_average_val)));
            maxMinTemps.put(min_average,new Text(String.valueOf(min_average_val)));
            context.write(key, maxMinTemps);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: stationAvgMax <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Avg Max Temp");
        job.setJarByClass(NoCombiner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setMapperClass(CombinerMapper.class);
        job.setCombinerClass(AvgStationTempCombiner.class);
        job.setReducerClass(AvgStationTempReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
