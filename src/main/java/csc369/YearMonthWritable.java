package csc369;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import java.util.Comparator;

public class YearMonthWritable implements WritableComparable<YearMonthWritable> {

    public IntWritable year;
    public IntWritable month;
    public static final String[] months = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

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