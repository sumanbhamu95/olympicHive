package olympicc;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NoOfParticipantsCountryWise {

	/**
	 * 1.total no. of partcipants in each country
	 */
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String arr[] = value.toString().split("\t");
			context.write(new Text(arr[2]), new Text(arr[0]));

		}
	}

	public static class MyReducer extends
			Reducer<Text, Text, Text, IntWritable> {
		public void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			
			
			HashSet h1=new HashSet();
			//int count = 0;
			for (Text a : value) {

				h1.add(a.toString());
				
				
			}
			/*int cc=0;
			Iterator itr=h1.iterator();
			while(itr.hasNext()){
				itr.next();
				cc++;
			}*/
			context.write(key, new IntWritable(h1.size()));
		}
	}

	public static void main(String args[]) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration obj = new Configuration();
		Job job = Job.getInstance(obj, "no. of participants country wise..");
		job.setJarByClass(NoOfParticipantsCountryWise.class);
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
