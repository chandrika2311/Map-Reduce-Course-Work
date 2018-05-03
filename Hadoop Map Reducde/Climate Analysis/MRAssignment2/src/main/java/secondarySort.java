import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class secondarySort {
    public secondarySort(){


    }

    public static class AvgStationTempMapper
            extends Mapper<Object, Text, stationYearTypePair, MapWritable> {
        private final int offset = 0;
        static Text Year_val = new Text("Year");
        static Text max_type = new Text("TMAX");
        static Text max_count = new Text("TMAX_COUNT");
        static Text min_type = new Text("TMIN");
        static Text min_count = new Text("TMIN_COUNT");
        static Text max_average = new Text("MAX_AVERAGE");
        static Text min_average = new Text("MIN_AVERAGE");

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            MapWritable StationDeets = new MapWritable();
            String station;
            String year;
            String date;
            String type;
            String Temp;
            int temp = 0;
            int year_int = 0;
//        --------------
            station = value.toString().split(",")[0];
            date = value.toString().split(",")[1];
            year = date.substring(0, 4);
            type = value.toString().split(",")[2];
            Temp = value.toString().split(",")[3];
            int year_val = Integer.parseInt(year);
            stationYearTypePair CompositeKey = new stationYearTypePair(new Text(station), new IntWritable(year_val));

            if (type.equals("TMAX")) {
                temp = Integer.parseInt(Temp);//Converting the maxTemp in string format to Integer format
                StationDeets.put(max_type, new IntWritable(temp));
                StationDeets.put(max_count, new IntWritable(1));
                StationDeets.put(min_type, new IntWritable(0));
                StationDeets.put(min_count, new IntWritable(0));
                context.write(CompositeKey, new MapWritable(StationDeets));
            } else if (type.equals("TMIN")) {
                temp = Integer.parseInt(Temp);//Converting the maxTemp in string format to Integer format
                year_int = Integer.parseInt(Temp);
                StationDeets.put(max_type, new IntWritable(0));
                StationDeets.put(max_count, new IntWritable(0));
                StationDeets.put(min_type, new IntWritable(temp));
                StationDeets.put(min_count, new IntWritable(1));
                context.write(CompositeKey, new MapWritable(StationDeets));
            }
        }}

        public static class AvgStationTempReducer
                extends Reducer<stationYearTypePair, MapWritable, Text, MapWritable> {
            static Text max_type = new Text("TMAX");
            static Text max_count = new Text("TMAX_COUNT");
            static Text min_type = new Text("TMIN");
            static Text min_count = new Text("TMIN_COUNT");

            String[] temps = new String[2];
            public AvgStationTempReducer(){


            }
            MapWritable maxMinTemps = new MapWritable();

            public void reduce(stationYearTypePair key, Iterable<MapWritable> values,
                               Context context
            ) throws IOException, InterruptedException {

                int tmax = 0;
                int tmin = 0;
                int tmax_count = 0;
                int tmin_count = 0;
                float max_average_val;
                float min_average_val;

                for (MapWritable val : values) {
                    tmax += ((IntWritable) val.get(max_type)).get();
                    tmax_count += ((IntWritable) val.get(max_count)).get();
                    tmin += ((IntWritable) val.get(min_type)).get();
                    tmin_count += ((IntWritable) val.get(min_count)).get();
                }
                try {
                    temps[1] = String.valueOf(tmax / (float) tmax_count);


                } catch (Exception e) {
                    temps[1] = "Nan";
                }
                try {
                    temps[0] = String.valueOf(tmin / (float) tmin_count);
                } catch (Exception e) {
                    temps[0] = "Nan";
                }



                String finalText = temps[0] + " , " + temps[1];
                maxMinTemps.put(new Text(String.valueOf(key.getYear())), new Text(finalText));


                context.write(key.getStation(), maxMinTemps);
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
        job.setJarByClass(secondarySort.class);
        job.setMapOutputKeyClass(stationYearTypePair.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setMapperClass(AvgStationTempMapper.class);
        job.setGroupingComparatorClass(groupComparatorOnStation.class);
        job.setPartitionerClass(secondSortPartitioner.class);
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


