package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

final String[] months = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

public class YearMonthWritable implements WritableComparable<YearMonthWritable> {

    private IntWritable year;
    private IntWritable month;

    @Override
    public int compareTo(YearMonthWritable other) {
        return ComparisonChain.start().compare(year, other.year).compare(month, other.month);
    }
}

public class RequestsPerMonth {

    public static final Class OUTPUT_KEY_CLASS = YearMonthWritable.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, YearMonthWritable, IntWritable> {
   	private final IntWritable one = new IntWritable(1);

        @Override
	protected void map(LongWritable key, Text value,
                Context context) throws IOException, InterruptedException {
	        String[] sa = value.toString().split(" ");
	        String[] date = sa[3].split("/");
            YearMonthWritable yearMonth = new YearMonthWritable();

            yearMonth.year.set(date[2].split(":")[0]);
            for (int i = 0; i < months.length; i++) {
                if (date[1].equals(months[i])) {
                    yearMonth.month.set(i + 1);
                }
            }
            
            context.write(yearMonth, one);
        }
    }

    public static class ReducerImpl extends Reducer<YearMonthWritable, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

        @Override
	protected void reduce(YearMonthWritable yearMonth, Iterable<IntWritable> counts,
			    Context context) throws IOException, InterruptedException {
            int sum = 0;
            Text yearMonthText = new Text();
            yearMonthText.set(toString(yearMonth.year) + "-" + months[yearMonth.month - 1]);
            Iterator<IntWritable> itr = counts.iterator();

            while (itr.hasNext()){
                sum += itr.next().get();
            }
            result.set(sum);
            context.write(yearMonthText, result);
        }
    }

}
