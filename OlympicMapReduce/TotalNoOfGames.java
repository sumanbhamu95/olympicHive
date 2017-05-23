package olympicc;

import java.io.IOException;
import java.util.HashSet;

import olympicc.NoCountriesparticipated.MyMapper;
import olympicc.NoCountriesparticipated.MyReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TotalNoOfGames {

	/**
	 * 4.total no. of games in olympics
	 */
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String arr[] = value.toString().split("\t");
			context.write(new Text("Total no. of games in olympic"), new Text(arr[5]));

		}
	}

	public static class MyReducer extends
			Reducer<Text, Text, Text, IntWritable> {
		public void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			
			
			HashSet h1=new HashSet();
			
			for (Text a : value) {

				h1.add(a.toString());
				
				
			}
			
			context.write(key, new IntWritable(h1.size()));
		}
	}

	public static void main(String args[]) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration obj = new Configuration();
		Job job = Job.getInstance(obj, "total no. of games..");
		job.setJarByClass(TotalNoOfGames.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileSystem.get(obj).delete(new Path(args[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}



}
