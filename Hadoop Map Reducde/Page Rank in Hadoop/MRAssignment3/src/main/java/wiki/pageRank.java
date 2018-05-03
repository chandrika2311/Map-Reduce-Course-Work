package wiki;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.xml.sax.*;
import java.util.HashSet;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
//Counters for incrementing between jobs
public class pageRank {
    public enum deltaCounterValue{
        Counter;
    }
    public enum numOfNodes{
        Counter;
    }
//-------------------------------------     PRE - PROCESSING JOB  Mapper       ---------------------------------------//
//      1. This is the first step where the .bz2 file is parsed using the parser provided.
    //  2. Upon parsing the mapper emits the key as a string and adjecency list also as a string.

//-------------------------------------     PRE - PROCESSING JOB  Mapper       ---------------------------------------//
    public static class preprecessingMapper
            extends Mapper<Object, Text, Text, Text> {
        private static Pattern namePattern;
        private static Pattern linkPattern;
        //---------------Setup function
        public void setup(Context context){
                // Keep only html pages not containing tilde (~).
                namePattern = Pattern.compile("^([^~]+)$");
               // Keep only html filenames ending relative paths and not containing tilde (~).
                linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
        }

        List adjList = new ArrayList<String>();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            // Configure parser.
            try {
                SAXParserFactory spf = SAXParserFactory.newInstance();
                spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
                SAXParser saxParser = spf.newSAXParser();
                XMLReader xmlReader = saxParser.getXMLReader();
                // Parser fills this list with linked page names.
                Set<String> linkPageNames = new HashSet<String>();
                xmlReader.setContentHandler(new WikiParser(linkPageNames));
                String line;
                // Each line formatted as (Wiki-page-name:Wiki-page-html).
                int delimLoc = value.toString().indexOf(':');
                String pageName = value.toString().substring(0, delimLoc);
                String html = value.toString().substring(delimLoc + 1);
                html = html.replace("&", "&amp;");
                Matcher matcher = namePattern.matcher(pageName);
                if (matcher.find()) {
                    // Skip this html file, name contains (~).
                    // Parse page and fill list of linked pages.
                    linkPageNames.clear();
                        try {
                            xmlReader.parse(new InputSource(new StringReader(html)));
                        } catch (SAXParseException e) {
                            e.printStackTrace();
                        }
                        String links = String.join(",", linkPageNames);
                        String[] linkList =  links.split(",");
                        if(linkList.length >1){
                            for(int i = 0; i<linkList.length;i++){
                                context.write(new Text(linkList[i]), new Text(""));
                            }
                        }
                        context.write(new Text(pageName), new Text(links));

                }
            } catch (SAXNotSupportedException e) {
                e.printStackTrace();
            } catch (SAXNotRecognizedException e) {
                e.printStackTrace();
            } catch (SAXException e) {
                e.printStackTrace();
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            }
        }
    }
//-------------------------------------     PRE - PROCESSING JOB  Mapper 2      ---------------------------------------//
    // 1. this is the Mapper that runs after the first Map=Reduce job
    // 2. this Map function runs as a Map Only job.
    // 3. Used to only append the initial page rank values to the end of each line.
//-------------------------------------     PRE - PROCESSING JOB  Mapper 2      ---------------------------------------//
    public static class preprecessingMap2
            extends Mapper<Object, Text, Text, Text> {
        double sizeOfgraph=0.0;
        Counter sizeOfgraph1;
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            sizeOfgraph = conf.getDouble("NumOfNodes",0.0);
            sizeOfgraph1 = context.getCounter(numOfNodes.Counter);
        }
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //value is a Text
            try{
            String[] hello = String.valueOf(value).split("\t");
            if (hello.length != 0){
                String keyValue = hello[0];
                if(hello.length >1){
                    String val =  String.valueOf(value).split("\t")[1];
                    String newString = val+":"+String.valueOf((double)1/sizeOfgraph);
                    context.write(new Text(keyValue), new Text (newString));
                }else{
                    String newString = ":"+String.valueOf((double)1/sizeOfgraph);
                    context.write(new Text(keyValue), new Text (newString));
                }
            }

        }catch(Exception e){
                e.printStackTrace();
            }

        }
    }
//-------------------------------------     PRE - PROCESSING JOB  Reducer      ---------------------------------------//
    // 1. This is the Reducer for the 1st job
    // 2. the total number of nodes in the graph are calculated here using the numOfNodes.Counter.
    // 3. The reducer emits NameOdNodes as key and adjecencyList as string in value.
////-------------------------------------     PRE - PROCESSING JOB  Reducer      ---------------------------------------//
    public static class preprecessingReducer
            extends Reducer<Text, Text, Text, Text> {
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        double sizeOfgraph=0.0;
        Configuration conf = context.getConfiguration();
        sizeOfgraph = conf.getDouble("NumOfNodes",0.0);
        context.getCounter(numOfNodes.Counter).setValue(0);
    }
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            String temp = "";
            for(Text vals : values){
                temp = temp + vals.toString();
            }
            Text newValues =new Text(temp.trim());
            context.getCounter(numOfNodes.Counter).increment((long) 1);
            context.write(key,newValues);
        }}
//-----------------------------         PAGE RANK JOB  Mapper       ----------------------------------------------------
    //1. this is the Mapper for PageRanking job.
    //2. Here we are spliting the string found into key and value pairs with page rank of each line.
    //3. New Page rank values are calculated using the old page rank values found.
    //4. In case the key has no links in its adjecency list, an empty list is emitted to the reducer.
    //  At the same time, creating a delta counter variable which gets incremented by the page Rank value of the node.
    //5. Else, the key along with the adjecency list and pageRank values are emitted to the reducer.
//       Also, the page rank value of a node is distributed to all its links n the adjecency list.
//-----------------------------         PAGE RANK JOB  Mapper       ----------------------------------------------------
    public static class pageRankingMapper
            extends Mapper<Object, Text, Text, Text> {
        double oldDeltaCounterValue;
        double DeltaCounterValue;
        double sizeOfgraph1;

        protected void setup(Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            oldDeltaCounterValue = conf.getDouble("deltaCounterValue",0.0);
            sizeOfgraph1 = conf.getDouble("numOfNodes",0.0);
            context.getCounter(deltaCounterValue.Counter).setValue(0);
        }
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            DeltaCounterValue=0.0;
            String value1 = String.valueOf(value);
            String[] WholeValue = value1.split("\t");

            Node node = new Node();
            String pageName = WholeValue[0];
            node.setNodeId(pageName);

            String[] pgRank = WholeValue[1].split(":");
            node.setAdjecencylist(pgRank[0]);

            node.setPageRank(Double.valueOf(pgRank[1]),oldDeltaCounterValue,sizeOfgraph1);
            String nodeString =node.getNodeId()+";"+String.valueOf(node.getAdjecencylist())+";"+String.valueOf(node.getPageRank());
            context.write(new Text(pageName),new Text(nodeString));
//5.
            if(node.getAdjecencylist().size() != 0){
                double p = node.getPageRank()/(double)node.getAdjecencylist().size();
                for(int i = 0; i < node.getAdjecencylist().size();i++){
                    String name = node.getAdjecencylist().get(i);

                    context.write(new Text(name), new Text(Double.toString(p)));
                }
//4.
            }else{
                DeltaCounterValue =node.getPageRank();
                context.getCounter(deltaCounterValue.Counter).increment((long) (DeltaCounterValue * Math.pow(10,12)));
            }
        }
    }
//-----------------------------------------    PAGE RANK JOB Reducer    ------------------------------------------------
// 1. Upon receiving the values from the Mapper, reducer checks if its a node with its adjecency list or
//    a link with its new page rank value
// 2. If its the link with page rank values they contribute to the running sum
// 3. If they are the link with adjecency list+ page rank, then create a new Node and assign a new pageRank value to it
// using the new runnning sum.
//4.Append the new pagerank values to the Node in string format and emit.


//-----------------------------------------    PAGE RANK JOB Reducer    ------------------------------------------------

    public static class pageRankingReducer
            extends Reducer<Text, Text, Text, Text> {
        double sizeOfgraph1=0.0;
        protected void setup(Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();

            sizeOfgraph1 = conf.getDouble("numOfNodes",0.0);
            context.getCounter(deltaCounterValue.Counter).setValue(0);
        }
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            double runningSum = 0.0;
            double alpha = 0.15;
            Node M = new Node();

            for(Text vals: values){
//1. Check whats bee received
                if (String.valueOf(vals).contains(";")){
//3.The node object was found: recover graph structure
                    String[] node = String.valueOf(vals).split(";");
                    M.setNodeId(node[0]);
                    M.setAdjecencylist(node[1]);
                }else{
//2.A PageRank contribution from an inlink was found: add it to the running sum
                    double p = Double.valueOf(String.valueOf(vals));
                    runningSum += p;
                }
            }
            double newPageRank = (alpha/(double)sizeOfgraph1)+((1-alpha)*runningSum);

            String hello = String.valueOf(key);
            if(hello =="%s"){
                System.out.println("New Page Rank: "+String.valueOf(key)+ " : "+ newPageRank);
            }
            M.setPageRank(newPageRank);
            String adjListToString = String.valueOf(M.getAdjecencylist());
            try{
                adjListToString = adjListToString.replace("[", "");
                adjListToString = adjListToString.replace("  ", "");
                adjListToString = adjListToString.replace("]", "");

                String nodeString = adjListToString + ":" + String.valueOf(M.getPageRank());
//4.
                context.write(new Text(M.getNodeId()),new Text(nodeString));
            }catch(Exception e){
                e.printStackTrace();
            }

        }
    }
//----------------------------------------------------------------------------------------------------------------------
//----------------------------------------------------------------------------------------------------------------------
//---------------------------------------------DRIVER PROGRAM-----------------------------------------------------------
//----------------------------------------------------------------------------------------------------------------------
//----------------------------------------------------------------------------------------------------------------------
    public static void main(String[] args) throws Exception {
        double sizeOfgraph=0.0;
        Configuration conf1 = new Configuration();
        conf1.setDouble("NumOfNodes",sizeOfgraph);
//---------------------------------------------Job1(Pre-processing job)-------------------------------------------------
        FileSystem fs = FileSystem.get(conf1);
        Job job1 = Job.getInstance(conf1, "Preprocessing");
        job1.setJarByClass(pageRank.class);
        job1.setMapperClass(pageRank.preprecessingMapper.class);
        job1.setReducerClass(pageRank.preprecessingReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);
        sizeOfgraph = job1.getCounters().findCounter(numOfNodes.Counter).getValue();
        System.out.println("Number OF Nodes job1: "+sizeOfgraph);

//---------------------------------------------Job(Map Only job)-------------------------------------------------------
//        Configuration confn = new Configuration();
//        FileSystem fsn = FileSystem.get(confn);
//        confn.setDouble("NumOfNodes",sizeOfgraph);
//        Job jobn = Job.getInstance(confn, "Preprocessing");
//        jobn.setJarByClass(pageRank.class);
//        jobn.setMapperClass(pageRank.preprecessingMap2.class);
//        jobn.setOutputKeyClass(Text.class);
//        jobn.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(jobn, new Path(args[1]));
//        FileOutputFormat.setOutputPath(jobn, new Path(args[1]+"preprocess1"));
//        jobn.waitForCompletion(true);
//        System.out.println("Number OF Nodes jobn: "+sizeOfgraph);

//---------------------------------------------Job(Page Rank job)-------------------------------------------------------
//        Configuration conf2 = new Configuration();
//        double Delta = 0.0;
//        for(int i = 1; i<=10; i++) {
//            conf2.setDouble("deltaCounterValue",Delta/Math.pow(10,12));
//            conf2.setDouble("numOfNodes",sizeOfgraph);
//            Job job2 = Job.getInstance(conf2, "Page Rank");
//            job2.setJarByClass(pageRank.class);
//            job2.setMapperClass(pageRank.pageRankingMapper.class);
//            job2.setReducerClass(pageRank.pageRankingReducer.class);
//
//            job2.setOutputKeyClass(Text.class);
//            job2.setOutputValueClass(Text.class);
//            if(i==1){
//                FileInputFormat.addInputPath(job2, new Path(args[1]+"preprocess1"));
//            }else{
//                FileInputFormat.addInputPath(job2, new Path(args[1] + String.valueOf(i-1)));
//            }
//            FileOutputFormat.setOutputPath(job2, new Path(args[1] + String.valueOf(i)));
//
//            job2.waitForCompletion(true);
//            Delta = job2.getCounters().findCounter(deltaCounterValue.Counter).getValue();
//        }
//---------------------------------------------Job(Top 100 Pages job)-------------------------------------------------------
//        Configuration conf4 = new Configuration();
//        Job job4 = Job.getInstance(conf4, "Top 100 Page Rank");
//        job4.setJarByClass(pageRank.class);
//        job4.setMapperClass(top100pageRank.top100pageRanksMapper.class);
//        job4.setReducerClass(top100pageRank.top100pageRanksReducer.class);
//        job4.setOutputKeyClass(DoubleWritable.class);
//        job4.setOutputValueClass(Text.class);
//        job4.setNumReduceTasks(1);
//        FileInputFormat.addInputPath(job4, new Path(args[1]+"10"));
//        FileOutputFormat.setOutputPath(job4, new Path(args[1]+"top100"));
//        job4.waitForCompletion(true);
    }}

//----------------------------------------------------------------------------------------------------------------------
//----------------------------------------------------------------------------------------------------------------------
//----------------------------------------------------------------------------------------------------------------------
//----------------------------------------------------------------------------------------------------------------------