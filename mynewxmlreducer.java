//package myxmlparser;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class mynewxmlreducer extends Reducer<Text,NullWritable, Text, IntWritable> {
	int count=0,other_tweet=0,mycount=0;
	public void reduce(Text key,Iterable<NullWritable> values,Context context) throws IOException,InterruptedException
	{
		
		count=count+1;
		String s=key.toString();
		//CharSequence a="#";
		int mycount=StringUtils.countMatches(s,"#");
		other_tweet=other_tweet+mycount;
		
	}
	public void run(Context context) throws IOException, InterruptedException 
	{
		setup(context);
		while(context.nextKeyValue())
		{
			reduce(context.getCurrentKey(), context.getValues(), context);
		}
		context.write(new Text("Number of Tweets on the topic"),new IntWritable(count));
		context.write(new Text("Number of other Tweets on the topic"),new IntWritable(other_tweet));
	}
}


