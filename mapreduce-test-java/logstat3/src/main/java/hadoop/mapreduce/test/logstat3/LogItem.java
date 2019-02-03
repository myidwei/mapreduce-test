package hadoop.mapreduce.test.logstat3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * Created by xiaoweiliang on 2019/2/3.
 */
public class LogItem implements WritableComparable<LogItem> {

    private String data;
    private int count;

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String toString(){
        String[] array = data.split("\\|");
        if(array.length >= 2){
            return array[0] + ":00 - " + array[1];
        }
        return data;
    }

    @Override
    public int compareTo(LogItem o) {
        if(data.compareTo(o.getData()) == 0){
            return Integer.compare(count,o.getCount());
        }else{
            return data.compareTo(o.getData());
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(data);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        data = dataInput.readUTF();
        count = dataInput.readInt();
    }
}
