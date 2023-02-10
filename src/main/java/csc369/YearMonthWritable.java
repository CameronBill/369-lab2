package csc369;

import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import java.util.Comparator;

public class YearMonthWritable implements WritableComparable<YearMonthWritable> {

    public IntWritable year;
    public IntWritable month;
    public static final String[] months = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

    @Override
    public void readFields(YearMonthWritable in) throws IOException {
        year = in.readUTF();
        month = in.readUTF();
    }

    @Override
    public void write(YearMonthWritable out) throws IOException {
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