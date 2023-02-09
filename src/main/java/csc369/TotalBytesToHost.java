package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalBytesToHost {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final Text hardCodedIP = new Text("64.242.88.10");

        @Override
	protected void map(LongWritable key, Text value,
                Context context) throws IOException, InterruptedException {
	        String[] sa = value.toString().split(" ");
	        Text clientIP = new Text();
            IntWritable bytesSent = new IntWritable();
            clientIP.set(sa[0]);
            bytesSent.set(Integer.parseInt(sa[9]));
            if clientIP.equals(hardCodedIP) {
                context.write(clientIP, bytesSent);
            }
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

        @Override
	protected void reduce(Text clientIP, Iterable<IntWritable> counts,
			    Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = counts.iterator();

            while (itr.hasNext()){
                sum += itr.next().get();
            }
            result.set(sum);
            context.write(clientIP, result);
        }
    }

}
