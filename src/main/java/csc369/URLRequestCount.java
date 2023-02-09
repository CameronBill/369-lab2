package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class URLRequestCount {

    public static final Class OUTPUT_KEY_CLASS = LongWritable.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final IntWritable one = new IntWritable(1);

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] entryArray = value.toString().split(" ");
	    Text url = new Text();
        url.set(entryArray[6]);
	    context.write(url, one);
        }
    }

    
    public static class ReducerImpl extends Reducer<Text, IntWritable, LongWritable, Text> {
    private LongWritable result = new LongWritable();

        @Override
	protected void reduce(Text url, Iterable<IntWritable> counts,
			      Context context) throws IOException, InterruptedException {
            
            int sum = 0;
            Iterator<Text> itr = urls.iterator();

            while (itr.hasNext()){
                sum += itr.next().get();
            }
            
            result.set(sum);
            context.write(result, url);
        }
    }

}
