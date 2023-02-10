package csc369;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.DataInput;
import org.apache.hadoop.io.DataOutput;
import java.util.Comparator;

public class YearMonthWritable implements WritableComparable<YearMonthWritable> {

    public IntWritable year;
    public IntWritable month;
    public static final String[] months = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

    @Override
    public void readFields(DataInput in) throws IOException {
        year = in.readUTF();
        month = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(year);
        out.writeUTF(month);
    }

    @Override
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