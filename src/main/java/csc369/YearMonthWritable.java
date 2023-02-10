package custom;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import java.util.Comparator;
import java.util.Comparator.comparing;

public class YearMonthWritable implements WritableComparable<YearMonthWritable> {

    public IntWritable year;
    public IntWritable month;
    public static final String[] months = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

    private static final Comparator<YearMonthWritable> YMCMP =
        comparing((YearMonthWritable yearMonth) -> yearMonth.year).thenComparing(yearMonth -> yearMonth.month);

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
        return YMCMP.compare(this, that);
    }
}