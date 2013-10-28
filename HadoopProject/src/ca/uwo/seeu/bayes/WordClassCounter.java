package ca.uwo.seeu.bayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import ca.uwo.seeu.hadoop.WordCount.IntSumReducer;

//import ca.uwo.seeu.hadoop.jobs.OneLineInputFormat;

public class WordClassCounter {	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 12)
		{
		      System.err.println("Usage: WordClassCounter [-schema <db_schema>] [-dataset_tb <html_tb>] [-idfolder <id_folder>] [-outfolder <out_folder>] [-chunk_size <chunk_size>] [-key_type <key_type>]");
		      return ;
		    }		  
		  String db_schema = "";		  
		  String idfolder = "";		  		  
		  String dataset_tb = "";		  
		  String outfolder = "";
		  String chunk_size = "";
		  String key_type = "";
		// TODO Auto-generated method stub
		    for (int i = 0; i < otherArgs.length; i++) {
		    	if (otherArgs[i].equals("-schema")) {
		    	  db_schema = otherArgs[++i];
		      }else if (otherArgs[i].equals("-dataset_tb")) {
		    	  dataset_tb = otherArgs[++i];
		      }else if (otherArgs[i].equals("-idfolder")) {
		    	  idfolder = otherArgs[++i];
		      }			      
		      else if (otherArgs[i].equals("-outfolder")) {
		    	  outfolder = otherArgs[++i];
		      }
		      else if (otherArgs[i].equals("-chunk_size")) {
		    	  chunk_size = otherArgs[++i];
		      }
		      else if (otherArgs[i].equals("-key_type")) {
		    	  key_type = otherArgs[++i];
		      }
		    }
		    
		//conf.set("mapred.job.tracker", "local");
		//conf.set("fs.default.name", "file:///");

		conf.set("mapred.textoutputformat.separator", "<<>>");		
		//conf.setInt("mapred.tasktracker.map.tasks.maximum", 4);
		conf.set("db_schema", db_schema);
		conf.set("dataset_tb", dataset_tb);		
		conf.set("chunk_size", chunk_size);
		conf.set("key_type", key_type);
		//conf.setLong("mapred.task.timeout", 900000);//increasing timeout to 15 minutes
		
		Job job = new Job(conf, "SEEU WordClassCounter");
		job.setJarByClass(WordClassCounter.class);
		job.setMapperClass(WordClassCountMapper.class);
		job.setCombinerClass(WordClassCountReducer.class);
		job.setReducerClass(WordClassCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//user one line input format
		//job.setInputFormatClass(OneLineInputFormat.class);
		job.setInputFormatClass(NLineInputFormat.class);
				
		FileInputFormat.addInputPath(job, new Path(idfolder));
		FileOutputFormat.setOutputPath(job, new Path(outfolder));

		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}
}