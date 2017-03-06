import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class DataPreprocessingRating extends Configured implements Tool{
	
	
	   public static void main( String[] args) throws  Exception {
		      int res  = ToolRunner .run( new DataPreprocessingRating(), args);
		      System .exit(res);
		   }
	   
	   public int run( String[] args) throws  Exception {
		   
	      Job jobdata  = Job .getInstance(getConf(), "DataPreprocessingRating ");
	      jobdata.setJarByClass( this .getClass()); 
	      
	      FileInputFormat.addInputPaths(jobdata,  args[1]);
	      FileOutputFormat.setOutputPath(jobdata,  new Path(args[2]+"/out_sumratings/"));
	      jobdata.setMapperClass( Map .class);
	      jobdata.setReducerClass( Reduce .class);
	      jobdata.setMapOutputKeyClass(Text.class);
	      jobdata.setMapOutputValueClass(Text.class);
	      jobdata.setOutputKeyClass( Text .class);
	      jobdata.setOutputValueClass( Text .class);
	      Job jobmeanrating = Job .getInstance(getConf(), "DataPreprocessingmeanratings ");
	      jobmeanrating.setJarByClass( this .getClass()); 
	      long mean =0;
	      Job jobout = null;
	      Job jobinout = null;
	      Job jobFileMerger = null;
	      if(jobdata.waitForCompletion( true)){
	    	  //System.out.println("--------Job1 completed------");
	    	  jobmeanrating.setJarByClass( this .getClass()); 
	    	  jobmeanrating.setNumReduceTasks(1);
	    	  FileInputFormat.addInputPaths(jobmeanrating,  args[2]+"/out_sumratings/");
		      FileOutputFormat.setOutputPath(jobmeanrating,  new Path(args[2]+"/out_sumratings_sm/"));
		      jobmeanrating.setMapperClass( MapMeaning .class);
		      jobmeanrating.setReducerClass( ReduceMeaning .class);
		      jobmeanrating.setMapOutputKeyClass(Text.class);
		      jobmeanrating.setMapOutputValueClass(Text.class);
		      jobmeanrating.setOutputKeyClass( Text .class);
		      jobmeanrating.setOutputValueClass( Text .class);
	    	  
	      }
	      if(jobmeanrating.waitForCompletion( true)){
	    	 // System.out.println("--------Job2 completed------");
	    	  mean= jobmeanrating.getCounters().findCounter("mean", "mean").getValue();
	    	  jobinout = Job .getInstance(getConf(), "DataPreprocessingmeanratingsoutput ");
	    	  jobinout.setJarByClass( this .getClass()); 
	    	  FileInputFormat.addInputPaths(jobinout,  args[1]);
		      FileOutputFormat.setOutputPath(jobinout,  new Path(args[2]+"/inout/"));
		      jobinout.setMapperClass( MapInput .class);
		      jobinout.setReducerClass( ReduceInput .class);
		      jobinout.setMapOutputKeyClass(Text.class);
		      jobinout.setMapOutputValueClass(Text.class);
		      jobinout.setOutputKeyClass( Text .class);
		      jobinout.setOutputValueClass( Text .class);
	      }
	      if(jobinout.waitForCompletion( true)){
	    	  //System.out.println("--------Job3 completed------");
	    	  jobout = Job .getInstance(getConf(), "DataPreprocessingFinalOutput ");
	    	  jobout.setJarByClass( this .getClass()); 
	    	  FileInputFormat.addInputPaths(jobout,  args[2]+"/inout/");
		      FileOutputFormat.setOutputPath(jobout,  new Path(args[2]+"/outratings/"));
		      jobout.getConfiguration().set("mean", String.valueOf(mean));
		      jobout.setMapperClass( MapOutput .class);
		      jobout.setReducerClass( ReduceOutput .class);
		      jobout.setMapOutputKeyClass(Text.class);
		      jobout.setMapOutputValueClass(Text.class);
		      jobout.setOutputKeyClass( Text .class);
		      jobout.setOutputValueClass( Text .class);
	      }
	      Job localJob = Job.getInstance(getConf(), "DataPreprocessingUserProfile ");
	      if(jobout.waitForCompletion( true)){
	    	 // System.out.println("--------Job4 completed------");
	    	    localJob.setJarByClass( this .getClass()); 
	    	    
	    	    FileInputFormat.addInputPaths(localJob, args[0]);
	    	    FileOutputFormat.setOutputPath(localJob, new Path(args[2] + "/out/"));
	    	    localJob.setMapperClass(MapUserProfile.class);
	    	    localJob.setReducerClass(ReduceUserProfile.class);
	    	    localJob.setMapOutputKeyClass(Text.class);
	    	    localJob.setMapOutputValueClass(Text.class);
	    	    localJob.setOutputKeyClass(Text.class);
	    	    localJob.setOutputValueClass(Text.class);
	    	    
	      }
	      if(localJob.waitForCompletion( true)){
	    	 // System.out.println("--------Job5 completed------");
	    	  jobFileMerger =Job .getInstance(getConf(), "DataPreprocessingFinalMergerOutput "); 
	    	  jobFileMerger.setJarByClass( this .getClass());
	    	  
		      
		      MultipleInputs.addInputPath(jobFileMerger, new Path(args[2]+"/outratings/"), TextInputFormat.class, RatingMapper.class);
		      MultipleInputs.addInputPath(jobFileMerger, new Path(args[2]+"/out/"), TextInputFormat.class, UserMapper.class);
		      
		      
		      FileOutputFormat.setOutputPath(jobFileMerger, new Path(args[2]+"/outmerger/"));

		      jobFileMerger.setReducerClass( FilesReducer .class);
		      jobFileMerger.setNumReduceTasks(1);
		      jobFileMerger.setOutputKeyClass( Text .class);
		      jobFileMerger.setOutputValueClass( Text .class);
	    	  
	      }
	      
	      return jobFileMerger.waitForCompletion( true)  ? 0 : 1;
	   }
	   
	   // Processing the Input file of User's profile
	   
	   
	   public static class MapUserProfile
	    extends Mapper<LongWritable ,  Text ,  Text ,  Text >
	  {
	    public void map( LongWritable offset,  Text lineText,  Context context)
	      throws IOException, InterruptedException
	    {
	      String str1 = lineText.toString();
	      Text localText = new Text();
	      String[] arrayOfString1 = str1.split(";");
	      String[] arrayOfString2 = arrayOfString1[1].split("\\s+");
	      String str2 = arrayOfString2[(arrayOfString2.length - 1)].trim();
	      String str3 = "unknown";
	      if (arrayOfString1.length == 3) {
	        str3 = arrayOfString1[2].equals("NULL") ? str3 : arrayOfString1[2].trim();
	      }
	      localText = new Text(str2 + "~~~~~" + str3);
	      context.write(new Text(arrayOfString1[0]), localText);
	    }
	  }
	  
	  public static class ReduceUserProfile
	    extends Reducer<Text, Text, Text, Text>
	  {
	    public void reduce( Text word,  Iterable<Text > counts,  Context context)
	      throws IOException, InterruptedException
	    {
	      for (Text localText : counts) {
	    	  context.write(word, localText);
	      }
	    }
	  }
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   // This Mapper is used to extract the each lines of the input file and parse it for book and ratings and in

	   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
		   
		   public void map( LongWritable offset,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {
	         String wordline  = lineText.toString();
	         Text currentRatings  = new Text();
	         String[] inputSequence = wordline.split(";");
	         //String[] c_id2 = inputSequence[1].split("\\s+") ;
	         String book_id = inputSequence[1].trim();
	         String defaultRating = "0";
	          if(inputSequence.length==3){
	        	  defaultRating = inputSequence[2].equals("NULL")||inputSequence[2].equals("")?defaultRating:inputSequence[2].trim();
	        	  defaultRating = defaultRating.replace("\"", "");
	          }
	         
	          currentRatings  = new Text(defaultRating);
	            context.write(new Text(book_id.trim()),currentRatings);
	      }
	   }
	   // This is used for combining all the books introduce the delimiters
	   public static class Reduce extends Reducer<Text ,  Text ,  Text ,  Text > {	
	      @Override 
	      public void reduce( Text word,  Iterable<Text > counts,  Context context)
	         throws IOException,  InterruptedException {
	    	  int ratings=0;
	    	  for(Text count: counts){
	    		  
	    		  ratings = Integer.parseInt(count.toString());
	    	  }
	    	  context.write(new Text("book"),  new Text(String.valueOf(ratings)));  
	      }
	   }
	   
	   // This Mapper is used to find the mean of the overall ratings in the system 
	   public static class MapMeaning extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
		   
		   public void map( LongWritable offset,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {
	         String wordline  = lineText.toString();
	         Text currentRatings  = new Text();
	         String[] inputSequence = wordline.split("\\s+");
	         //String[] c_id2 = inputSequence[1].split("\\s+") ;
	         String book_id = inputSequence[0].trim();
	         
	          currentRatings  = new Text(inputSequence[1].trim());
	            context.write(new Text(book_id),currentRatings);
	      }
	   }
	   // This is used for combining all the book's rating and finding the mean of all the rating.
	   public static class ReduceMeaning extends Reducer<Text ,  Text ,  Text ,  Text > {	
		   
		   int ratings=0;
	       int totalCount =0;
	       long inc = 5;
	      @Override 
	      public void reduce( Text word,  Iterable<Text > counts,  Context context)
	         throws IOException,  InterruptedException {
	    	  int rate=0;
	    	  
	    	  for(Text count: counts){
	    		  rate = Integer.parseInt(count.toString());
	    		  if(rate!=0){
		    		  ratings =ratings+ rate;
		    		  totalCount++;
	    	  }
	    	  }
	    	  //context.write(word,  new Text(String.valueOf(ratings)+"-----"+totalCount));
	    	  inc = (ratings/totalCount);
	    	
	    	  
	      }
	      
	    	protected void cleanup(Context context) throws IOException, InterruptedException {
	      		// Incrementing the counter for accessing the nodecount in the driver class.
	      		context.getCounter("mean","mean").increment(inc);
	   } 
	   }
	   

	// This Mapper is used to extract the each lines of the input file and parse it for null ratings and in
	// introduce the delimiters and 
	public static class MapInput extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
		   
		   public void map( LongWritable offset,  Text lineText,  Context context)
	     throws  IOException,  InterruptedException {
	      String wordline  = lineText.toString();
	      Text currentWord  = new Text();
	      String[] inputSequence = wordline.split(";");
	      //String[] c_id2 = inputSequence[1].split("\\s+") ;
	      String book_id = inputSequence[1].trim();
	      String defaultRating = "0";
	       if(inputSequence.length==3){
	     	  defaultRating = inputSequence[2].equals("NULL")||inputSequence[2].equals("")?defaultRating:inputSequence[2].trim();
	     	 defaultRating = defaultRating.replace("\"", "");
	       }
	      
	         currentWord  = new Text(book_id+"######"+defaultRating+"&&&&&");
	         context.write(new Text(inputSequence[0].trim()+"&&&&&"),currentWord);
	   }
	}
	// This is used for combining all the 
	public static class ReduceInput extends Reducer<Text ,  Text ,  Text ,  Text > {	
	   @Override 
	   public void reduce( Text word,  Iterable<Text > counts,  Context context)
	      throws IOException,  InterruptedException {
	 	  StringBuffer sb = new StringBuffer();
	 	  for(Text count: counts){
	 		  
	 		  sb.append(count.toString());
	 	  }
	 	  context.write(new Text(word.toString().trim()),  new Text(sb.toString()));  
	   }
	}
	
	
	
	// This Mapper is used to extract the each lines of the input file and do the processing with the calculated mean
	// introduce the delimiters and 
	public static class MapOutput extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
		   
		   public void map( LongWritable offset,  Text lineText,  Context context)
	     throws  IOException,  InterruptedException {
	      String wordline  = lineText.toString();
	      Text currentWord  = new Text();
	      String[] inputSequence = wordline.split("&&&&&");
	      Configuration conf = context.getConfiguration();
	      //String[] c_id2 = inputSequence[1].split("\\s+") ;
	      StringBuffer sb = new StringBuffer();
	      int mean = Integer.parseInt(conf.get("mean"));
	      for(int i =1;i<inputSequence.length;i++)
	      {
	    	  String[] book_id_rate = inputSequence[i].split("######");
	    	  sb.append(book_id_rate[0]+"######"+(Integer.parseInt(book_id_rate[1])-mean)).append("        ");
	      }
	      
	         currentWord  = new Text(sb.toString());
	         context.write(new Text(inputSequence[0].trim()+"&&&&&"),currentWord);
	   }
	}
	// This is used for combining all the User ids together
	public static class ReduceOutput extends Reducer<Text ,  Text ,  Text ,  Text > {	
	   @Override 
	   public void reduce( Text word,  Iterable<Text > counts,  Context context)
	      throws IOException,  InterruptedException {
	 	  StringBuffer sb = new StringBuffer();
	 	  for(Text count: counts){
	 		  
	 		  sb.append(count.toString());
	 	  }
	 	  context.write(word,  new Text(sb.toString()));  
	   }
	}
	
	// Combining multiple file outputs to one file.
	// Two mapper used for combining the user data and the rating data
	
	public static class UserMapper extends Mapper<LongWritable ,  Text ,  Text ,  Text > 
	 {
	  public void map(LongWritable key, Text value, Context context)
	  throws IOException, InterruptedException
	  {
	   String[] line=value.toString().split("\\s+"); 

	   context.write(new Text(line[0].trim()+"&&&&&"), new Text(line[1]));
	  }
	 }
	
	public static class RatingMapper extends Mapper<LongWritable ,  Text ,  Text ,  Text > 
	 {
	  public void map(LongWritable key, Text value, Context context)
	  throws IOException, InterruptedException
	  {
	   String[] line=value.toString().split("&&&&&");
	   context.write(new Text(line[0].trim()+"&&&&&"), new Text(line[1].trim()));
	  }
	 }
	// The final reducer is the one which performs the combination of results from the file of user profile and the rating got from the previous mapper and reducer task.
	public static class FilesReducer extends Reducer<Text ,  Text ,  Text ,  Text >
	 {
	  String line=null;
	  
	  @Override 
	   public void reduce( Text word,  Iterable<Text > counts,  Context context)
	      throws IOException,  InterruptedException {
	 	  StringBuffer sb_user = new StringBuffer();
	 	 StringBuffer sb_ratings = new StringBuffer();
	 	 String user = null;
	 	 String ratings = null;
	 	  for(Text count: counts){
	 		  
	 		  if(count.toString().contains("~~~~~")){
	 			 sb_user.append(count.toString());
	 		  }
	 		  else if(count.toString().contains("######")){
	 			 sb_ratings.append(count.toString());
	 		  }
	 		  if(!sb_user.toString().equals("") || !sb_user.toString().isEmpty()){
	 			  
	 			 user= "&&&&&"+sb_user.toString();
	 		  }
	 		  if(!sb_ratings.toString().equals("") || !sb_ratings.toString().isEmpty()){
	 			  
	 			 ratings= "&&&&&"+sb_ratings.toString();
	 		  }
	 	  }
	 	  context.write(new Text(word.toString()),  new Text(ratings+user));  
	   }
	  
	  
	}
}
