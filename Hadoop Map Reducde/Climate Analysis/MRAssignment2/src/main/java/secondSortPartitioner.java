import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class secondSortPartitioner
        extends Partitioner<stationYearTypePair,Text> {
    public  secondSortPartitioner(){

    }

    @Override
    public int getPartition(stationYearTypePair pair, Text text, int i) {
        return Math.abs(pair.getStation().hashCode()%i);

    }
}