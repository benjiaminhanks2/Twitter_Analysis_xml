//package myxmlparser;

//import myxmlparser.myxmlparser.xmlInputFormat1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class mynewxmldriver {
	public static void main(String[] args) throws Exception
    {
		if(args.length!=2)
		{
		System.err.println("Usage: Worddrivernewapi <input path> <output path>");
		System.exit(-1);
		}
            Configuration conf = new Configuration();

            conf.set("xmlinput.start", "<title>");
            conf.set("xmlinput.end", "</title>");
            Job job = new Job(conf);
            job.setMapperClass(mynewxmlmaper.class);
            job.setReducerClass(mynewxmlreducer.class);
            job.setJarByClass(mynewxmldriver.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

         
         //   job.setNumReduceTasks(0);

            job.setInputFormatClass(xmlInputFormat1.class);
         //   job.setOutputFormatClass(IntWritableOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
