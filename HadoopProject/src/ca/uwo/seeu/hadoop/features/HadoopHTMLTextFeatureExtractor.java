package ca.uwo.seeu.hadoop.features;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//import ca.uwo.seeu.hadoop.jobs.OneLineInputFormat;

public class HadoopHTMLTextFeatureExtractor {	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 14)
		{
		      System.err.println("Usage: HadoopHTMLTextFeatureExtractor [-schema <db_schema>] [-html_tb <html_tb>] [-text_tb <text_tb>] [-idfolder <id_folder>] [-chunk_size <chunk_size>] [-key_type <key_type>] [-outfolder <outfolder>]");
		      return ;
		    }		  
		  String db_schema = "";		  
		  String idfolder = "";		  		  
		  String html_tb = "";
		  String text_tb = "";
		  String outfolder = "";
		  String chunk_size = "";
		  String key_type = "";
		// TODO Auto-generated method stub
		    for (int i = 0; i < otherArgs.length; i++) {
		    	if (otherArgs[i].equals("-schema")) {
		    	  db_schema = otherArgs[++i];
		      }else if (otherArgs[i].equals("-html_tb")) {
		    	  html_tb = otherArgs[++i];
		      }else if (otherArgs[i].equals("-text_tb")) {
		    	  text_tb = otherArgs[++i];
		      }	      
		      else if (otherArgs[i].equals("-idfolder")) {
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
		conf.set("html_tb", html_tb);
		conf.set("text_tb", text_tb);
		conf.set("chunk_size", chunk_size);
		conf.set("key_type", key_type);
		//conf.setLong("mapred.task.timeout", 900000);//increasing timeout to 15 minutes
		
		Job job = new Job(conf, "SEEU HadoopHTMLTextFeatureExtractor");
		job.setJarByClass(HadoopHTMLTextFeatureExtractor.class);
		job.setMapperClass(HTMLExtractorMapper.class);
		job.setNumReduceTasks(0);//do not use reduce task

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//user one line input format
		//job.setInputFormatClass(OneLineInputFormat.class);
		job.setInputFormatClass(NLineInputFormat.class);
				
		FileInputFormat.addInputPath(job, new Path(idfolder));
		FileOutputFormat.setOutputPath(job, new Path(outfolder));

		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}
}