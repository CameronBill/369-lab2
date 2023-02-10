package csc369;

import java.io.IOException;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import java.util.Comparator;

public class YearMonthWritable implements WritableComparable<YearMonthWritable> {

    public IntWritable year;
    public IntWritable month;
    public static final String[] months = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

    @Override
    public void readFields(DataInput.DataInputStream in) throws IOException {
        year.set(Integer.parseInt(in.readUTF()));
        month.set(Integer.parseInt(in.readUTF()));
    }

    public void write(DataOutputStream out) throws IOException {
        out.writeUTF(Integer.toString(year.get()));
        out.writeUTF(Integer.toString(month.get()));
    }

    public int compareTo(YearMonthWritable that) {
        int res = this.year.compareTo(that.year);
        if (res == 0) {
            return this.month.compareTo(that.month);
        }
        else {
            return res;
        }
    }
}