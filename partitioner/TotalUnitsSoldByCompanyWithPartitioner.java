package com.bigdata.acadgild;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TotalUnitsSoldByCompanyWithPartitioner {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Configuration con = new Configuration();
		Job job = new Job(con);
		job.setJarByClass(TotalUnitsSoldByCompanyWithPartitioner.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapperClass(TotalUnitsSoldByCompanyMapper.class);
		
		job.setPartitionerClass(TotalUnitsSoldByCompanyPartitioner.class);
		
		job.setNumReduceTasks(4);
		job.setReducerClass(TotalUnitsSoldByCompanyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
}
	
	public static class TotalUnitsSoldByCompanyMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private static final String NA = "NA"; 
		public void map(LongWritable key, Text value, Context context ) throws IOException,InterruptedException
		{
			String[] values = value.toString().split("\\|");
			if ( !NA.equals(values[0]) &&  !NA.equals(values[1]))  {
				context.write(new Text(values[0]), new IntWritable(1));
			}
		}
	}
	
	public static class TotalUnitsSoldByCompanyReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private Integer minValue = Integer.MIN_VALUE;
		private IntWritable total = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			
			Integer totalUnits = 0;
			for ( IntWritable value : values ) 
			{
				if ( value.get() > minValue )
				{
					totalUnits+=value.get();
				}
			}
			total.set(totalUnits);
			context.write(key, total);
		}
	}
	
	public static class TotalUnitsSoldByCompanyPartitioner extends Partitioner<Text, IntWritable>
	{
		private static final ArrayList<String> AF = (ArrayList<String>) Arrays.asList("A","B","C","D","E","F");
		private static final ArrayList<String> GL = (ArrayList<String>) Arrays.asList("G","H","I","J","K","L");
		private static final ArrayList<String> MR = (ArrayList<String>) Arrays.asList("M","N","O","P","Q","R");
		
		@Override
		public int getPartition(Text key, IntWritable value, int arg2) {
		
			String k = key.toString().toUpperCase().substring(0, 1);
			if ( AF.contains(k) ) 
			{		
					return 0;
			}
			else if ( GL.contains(k))
			{
				return 1;
			}
			else if ( MR.contains(k))
			{
					return 2;
			}
			else
			{
				return 3;
			}
		
		}
	}
}