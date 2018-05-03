import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.ArrayList;
import java.io.IOException;
import java.util.HashMap;

public class InMapperComb {

    static Text max_type = new Text("TMAX");
    static Text max_count = new Text("TMAX_COUNT");
    static Text min_type = new Text("TMIN");
    static Text min_count = new Text("TMIN_COUNT");
    static Text max_average = new Text("MAX_AVERAGE");
    static Text min_average = new Text("MIN_AVERAGE");
    public static class InMapperCombMapper
            extends Mapper<Object, Text, Text, MapWritable> {
        static HashMap<Text,ArrayList> combinedStationDeets = new HashMap<>();
        //Tryingt o change the implementation to MapWritable
//        private final static IntWritable one = new IntWritable();
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            ArrayList StationDeets=new ArrayList();
            String station;
            String type;
            String Temp;
//-----------------------Splittling the string to actual values--------------------------
            station = value.toString().split(",")[0];
            type = value.toString().split(",")[2];
            Temp = value.toString().split(",")[3];
//---------------------------------
            if(combinedStationDeets.containsKey(new Text(station))){

                StationDeets = combinedStationDeets.get(new Text(station));


                if (type.equals("TMAX") ){

                    int maxtempValfromFile =Integer.parseInt(Temp);
                    int tmaxVal = maxtempValfromFile+(int)StationDeets.get(0);
                    int tmaxCountVal= 1+(int) StationDeets.get(1);
                    StationDeets.set(0,tmaxVal);
                    StationDeets.set(1,tmaxCountVal);
                    combinedStationDeets.put(new Text(station),StationDeets);
                }
                else if(type.equals("TMIN") ){

                    int mintempValfromFile = (new IntWritable(Integer.parseInt(Temp))).get();
                    int tminVal = mintempValfromFile+(int)StationDeets.get(2);
                    int tminCountVal = 1+(int)StationDeets.get(3);
                    StationDeets.set(2,tminVal);
                    StationDeets.set(3,tminCountVal);
                    combinedStationDeets.put(new Text(station),StationDeets);
                }
            }else{

                if (type.equals("TMAX") ) {

                    int maxtempValfromFile = Integer.parseInt(Temp);


                    StationDeets.add(0,maxtempValfromFile);
                    StationDeets.add(1,1);

                    StationDeets.add(2,0);
                    StationDeets.add(3,0);

                    combinedStationDeets.put(new Text(station),StationDeets);
                }
                else  if (type.equals("TMIN") ) {

                    int mintempValfromFile = Integer.parseInt(Temp);

                    StationDeets.add(0,0);
                    StationDeets.add(1,0);

                    StationDeets.add(2,mintempValfromFile);
                    StationDeets.add(3,1);


                    combinedStationDeets.put(new Text(station),StationDeets);

                }
            }

//                context.write(new MapWritable(combinedStationDeets));
        }
        protected void cleanup(Context context)throws IOException,InterruptedException{

            for (HashMap.Entry<Text,ArrayList> entry : combinedStationDeets.entrySet()){
                Text key = entry.getKey();
                ArrayList StationDetails = entry.getValue();
                MapWritable Details = new MapWritable();

                Details.put(max_type, new IntWritable((Integer) StationDetails.get(0)));
                Details.put(max_count, new IntWritable((Integer) StationDetails.get(1)));
                Details.put(min_type, new IntWritable((Integer) StationDetails.get(2)));
                Details.put(min_count, new IntWritable((Integer) StationDetails.get(3)));


                context.write(key,Details);
            }
        }
    }

    public static class InMapperCombReducer
            extends Reducer<Text,MapWritable,Text,MapWritable> {
        MapWritable maxMinTemps = new MapWritable();
        int tmax = 0;
        int tmin = 0;
        int tmax_count=0;
        int tmin_count=0;
        public void reduce(Text key, Iterable<MapWritable> values,
                           Context context
        ) throws IOException, InterruptedException {


//            System.out.println("values"+values);
            for (MapWritable val : values) {
                try{
                    tmax += ((IntWritable) val.get(max_type)).get();
                    tmax_count += ((IntWritable) val.get(max_count)).get();
                }catch(Exception e){
                    tmax += 0;
                    tmax_count+=0;
                }
                try{
                    tmin += ((IntWritable) val.get(min_type)).get();
                    tmin_count += ((IntWritable) val.get(min_count)).get();
                }catch(Exception e){
                    tmin +=0;
                    tmin_count+=0;
                }
            }
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
        job.setMapperClass(InMapperCombMapper.class);

        job.setReducerClass(InMapperCombReducer.class);

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

