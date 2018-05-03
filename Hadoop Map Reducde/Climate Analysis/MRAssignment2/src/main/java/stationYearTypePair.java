import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class stationYearTypePair
        implements WritableComparable<stationYearTypePair> {
    private Text station;
    private IntWritable Year;

    //    private IntWritable temprature;
//    private IntWritable count_vals;
    public stationYearTypePair(){
        station = new Text();
        Year = new IntWritable();

    }
    public stationYearTypePair(Text val1,IntWritable val2){
        station =val1;
        Year = val2;

    }
    public void setStation(Text val){
        station =val;

    }
    public void setYear(IntWritable val){
        Year = val;
    }


    public Text getStation(){
        return this.station;
    }
    public IntWritable getYear(){

        return this.Year;
    }

    @Override
    public int compareTo(stationYearTypePair pair) {
        int compareValue1 = this.station.compareTo((pair.getStation()));

        if(compareValue1 ==0){
            compareValue1= Year.compareTo(pair.getYear());
        }
        return compareValue1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        station.write(dataOutput);
        Year.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        station.readFields(dataInput);
        Year.readFields(dataInput);
    }
}