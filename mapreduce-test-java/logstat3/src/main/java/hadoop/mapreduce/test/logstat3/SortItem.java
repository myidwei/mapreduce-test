package hadoop.mapreduce.test.logstat3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortItem extends WritableComparator {

    public SortItem() {
        super(LogItem.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        LogItem o1=(LogItem)a;
        LogItem o2=(LogItem)b;
        String d1 = o1.getData();
        String d2 = o2.getData();
        String[] a1 = d1.split("\\|");
        String[] a2 = d2.split("\\|");
        int result = Integer.compare(Integer.parseInt(a1[0]),Integer.parseInt(a2[0]));
        if(result != 0){
            return result;
        }
        return -Integer.compare(o1.getCount(),o2.getCount());
    }
}