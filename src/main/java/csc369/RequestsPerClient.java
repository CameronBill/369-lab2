package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class RequestsPerClient {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;
    public static final Class OUTPUT_KEY_CLASS2 = IntWritable.class;
    public static final Class OUTPUT_VALUE_CLASS2 = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final Text hardCodedURL = new Text("/robots.txt");
   	private final IntWritable one = new IntWritable(1);

        @Override
	protected void map(LongWritable key, Text value,
                Context context) throws IOException, InterruptedException {
	        String[] sa = value.toString().split(" ");
	        Text clientIP = new Text();
            Text url = new Text();
            
            clientIP.set(sa[0]);
            url.set(sa[6]);
            
            if (url.equals(hardCodedURL)) {
                context.write(clientIP, one);
            }
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, IntWritable, Text> {
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
            context.write(result, clientIP);
        }
    }

    public static class MapperImpl2 extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split("\t");
        IntWritable count = new IntWritable(Integer.parseInt(sa[0]));
	    Text clientIP = new Text();
        clientIP.set(sa[1]);

	    context.write(count, clientIP);
        }
    }

    public static class ReducerImpl2 extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
	protected void reduce(IntWritable count, Iterable<Text> urls,
			      Context context) throws IOException, InterruptedException {
            
            Iterator<Text> itr = urls.iterator();
            while (itr.hasNext()){
                context.write(count, itr.next());
            }
        }
    }

}
