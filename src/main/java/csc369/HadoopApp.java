package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Hadoop example");
		Boolean chaining = false;
		Job job2 = new Job(conf, "Secondary Job");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> <input dir> <output dir>");
	    System.exit(-1);
	} else if ("WordCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(WordCount.ReducerImpl.class);
	    job.setMapperClass(WordCount.MapperImpl.class);
	    job.setOutputKeyClass(WordCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(WordCount.OUTPUT_VALUE_CLASS);
	} else if ("AccessLog".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(AccessLog.ReducerImpl.class);
	    job.setMapperClass(AccessLog.MapperImpl.class);
	    job.setOutputKeyClass(AccessLog.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(AccessLog.OUTPUT_VALUE_CLASS);
    } else if ("AccessLog2".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(AccessLog2.ReducerImpl.class);
	    job.setMapperClass(AccessLog2.MapperImpl.class);
	    job.setOutputKeyClass(AccessLog2.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(AccessLog2.OUTPUT_VALUE_CLASS);
	} else if ("URLRequestCount".equalsIgnoreCase(otherArgs[0])) {
		chaining = true;
		job.setReducerClass(URLRequestCount.ReducerImpl.class);
	    job.setMapperClass(URLRequestCount.MapperImpl.class);
	    job.setOutputKeyClass(URLRequestCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(URLRequestCount.OUTPUT_VALUE_CLASS);

		job2.setReducerClass(URLRequestCount.ReducerImpl2.class);
		job2.setMapperClass(URLRequestCount.MapperImpl2.class);
		job2.setOutputKeyClass(URLRequestCount.OUTPUT_KEY_CLASS2);
		job2.setOutputValueClass(URLRequestCount.OUTPUT_VALUE_CLASS2);
	} else if ("ResponseCodeCount".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(ResponseCodeCount.ReducerImpl.class);
	    job.setMapperClass(ResponseCodeCount.MapperImpl.class);
	    job.setOutputKeyClass(ResponseCodeCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(ResponseCodeCount.OUTPUT_VALUE_CLASS);
	} else if ("TotalBytesToHost".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(TotalBytesToHost.ReducerImpl.class);
	    job.setMapperClass(TotalBytesToHost.MapperImpl.class);
	    job.setOutputKeyClass(TotalBytesToHost.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(TotalBytesToHost.OUTPUT_VALUE_CLASS);
	} else if ("RequestsPerClient".equalsIgnoreCase(otherArgs[0])) {
		chaining = true;
		job.setReducerClass(RequestsPerClient.ReducerImpl.class);
	    job.setMapperClass(RequestsPerClient.MapperImpl.class);
	    job.setOutputKeyClass(RequestsPerClient.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(RequestsPerClient.OUTPUT_VALUE_CLASS);

		job2.setReducerClass(RequestsPerClient.ReducerImpl2.class);
		job2.setMapperClass(RequestsPerClient.MapperImpl2.class);
		job2.setOutputKeyClass(RequestsPerClient.OUTPUT_KEY_CLASS2);
		job2.setOutputValueClass(RequestsPerClient.OUTPUT_VALUE_CLASS2);
	} else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}

	if (chaining) {
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path("temp_dir"));
		job.waitForCompletion(true);

		FileInputFormat.addInputPath(job2, new Path("temp_dir"));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
		System.exit(job2.waitForCompletion(true) ? 0: 1);
	} else {
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		System.exit(job.waitForCompletion(true) ? 0: 1);
	}
        
    }

}
