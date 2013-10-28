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

public class HadoopHTMLAnchorTextFeatureExtractor {	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 16)
		{
		      System.err.println("Usage: HadoopHTMLAnchorTextFeatureExtractor [-schema <db_schema>] [-url2id <url2id>] [-html_tb <html_tb>] [-text_tb <text_tb>] [-idfolder <id_folder>] [-chunk_size <chunk_size>] [-key_type <key_type>] [-outfolder <outfolder>]");
		      return ;
		    }		  
		  String db_schema = "";		  
		  String idfolder = "";		  		  
		  String html_tb = "";
		  String text_tb = "";
		  String outfolder = "";
		  String chunk_size = "";
		  String key_type = "";
		  String url2id = "";
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
		      else if (otherArgs[i].equals("-url2id")) {
		    	  url2id = otherArgs[++i];
		      }	
		    }
		    
		//conf.set("mapred.job.tracker", "local");
		//conf.set("fs.default.name", "file:///");

		conf.set("mapred.textoutputformat.separator", "<<>>");		
		
		//conf.setInt("mapred.tasktracker.map.tasks.maximum", 4);
		conf.set("mapred.child.java.opts", "-Xmx1024m");
		conf.set("seeu_url2id_fname", url2id);
		conf.set("db_schema", db_schema);
		conf.set("html_tb", html_tb);
		conf.set("text_tb", text_tb);
		conf.set("chunk_size", chunk_size);
		conf.set("key_type", key_type);
		//conf.setLong("mapred.task.timeout", 900000);//increasing timeout to 15 minutes
		
		Job job = new Job(conf, "SEEU HadoopHTMLAnchorTextFeatureExtractor");
		job.setJarByClass(HadoopHTMLAnchorTextFeatureExtractor.class);
		job.setMapperClass(HTMLAnchorExtractorMapper.class);
		job.setReducerClass(HTMLAnchorExtractorReducer.class);		

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