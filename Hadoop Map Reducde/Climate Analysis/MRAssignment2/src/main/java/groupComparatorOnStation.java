import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class groupComparatorOnStation
        extends WritableComparator {

    public groupComparatorOnStation() {

        super(stationYearTypePair.class, true);
    }

    public int compare(WritableComparable wc1, WritableComparable wc2) {
        stationYearTypePair pair = (stationYearTypePair) wc1;
        stationYearTypePair pair2 = (stationYearTypePair) wc2;
        return pair.getStation().compareTo(pair2.getStation());
    }
}