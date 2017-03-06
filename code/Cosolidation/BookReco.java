import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Iterator;
import java.util.HashMap;

public class BookReco extends Configured implements Tool {

	
   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new BookReco(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
	   String temp_out = args[1]+"/temp/";
	   
	   Path outputTempDir = new Path(temp_out);
      Job jobmov  = Job .getInstance(getConf(), "MovieData ");
      Job jobmov2 =null;
Job jobmov3 =null;
      jobmov.setJarByClass( this .getClass()); 
      
      FileInputFormat.addInputPaths(jobmov,  args[0]);
      FileOutputFormat.setOutputPath(jobmov,  outputTempDir);
      jobmov.setMapperClass( Map .class);
      jobmov.setReducerClass( Reduce .class);
      jobmov.setMapOutputKeyClass(Text.class);
      jobmov.setMapOutputValueClass(Text.class);
      jobmov.setOutputKeyClass( Text .class);
      jobmov.setOutputValueClass( Text .class);
      if(jobmov.waitForCompletion(true)){
    	  jobmov2 = Job .getInstance(getConf(), " MovieData ");
    	  jobmov2.setJarByClass(this .getClass());
    	 //job2.getConfiguration().set("nOD", String.valueOf(cs.getFileCount()));
    	 //job2.getConfiguration().set("tmp_out", tmp_out);
    	  jobmov2.setMapperClass( Map2 .class);
    	  jobmov2.setReducerClass( Reduce2 .class);
    	  jobmov2.setOutputKeyClass( Text .class);
    	  jobmov2.setOutputValueClass( Text .class);
    	  FileInputFormat.addInputPaths(jobmov2,  temp_out);
          FileOutputFormat.setOutputPath(jobmov2,  new Path(args[1]+"/out/"));
      

      }

      return jobmov2.waitForCompletion( true)  ? 0 : 1;
   }
   
//this mapper has output key as country and book and values as recommended books for that book or rating and the user who has rated it
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
	   
	   public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
     String wordline  = lineText.toString();
         Text currentWord  = new Text();
         String[] inputSequence;
if(wordline.contains("&&&&&"))
{
inputSequence= wordline.split("&&&&&");


if(inputSequence.length==4)
{


if(inputSequence[3].contains("~~~~~"))
{
String[] agecountry=inputSequence[3].trim().split("~~~~~");
String country=agecountry[0].trim();
if(inputSequence[2].toLowerCase().contains("######"))
{
String[] allrating=inputSequence[2].trim().split("\\s+");
for(int i=0;i<(allrating.length);i++)
{
if(allrating[i].contains("######"))
{
String[] rating=allrating[i].trim().split("######");
//context.write(new Text(country+"######"+inputSequence[0]),new Text(""));
  context.write(new Text(country+"######"+rating[0]),new Text(inputSequence[0]+"######"+rating[1]));

}
} 
}
}
}
} 
if(!(wordline.contains("&&&&&")))
{
String[] recommend=wordline.trim().split("\\s+");
String[] values=recommend[0].split(",");
context.write(new Text(values[0]+"######"+values[1]),new Text(recommend[1]));

}
    }

   }

//this reducer has output key as user and value as the book he has rated and the recommended books iwth correlation score. 
   public static class Reduce extends Reducer<Text ,  Text ,  Text ,  Text > {
      @Override 
      public void reduce( Text word,  Iterable<Text > counts,  Context context)
         throws IOException,  InterruptedException {
ArrayList<String> users=new ArrayList<>();    	  
String rec="";
String[] books=word.toString().split("######");
    	  StringBuffer sb  = new StringBuffer();
         for ( Text rating  : counts) {
if(rating.toString().contains("######"))
{
String[] allusers=rating.toString().split("######");
users.add(rating.toString());
}
else
{
rec=rating.toString();
}            
        	 } 
for(String all : users)
{
        context.write(new Text(all.trim()+"######"+books[1].trim()),new Text(rec.trim()));
 }        
      }
   }
   




   //Following mapper and reducer combines the recommendations of all the books the user has rated and sorets them according to their correlation score and ratings
   public static class Map2 extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
	      

	      public void map( LongWritable offset,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {
	       	  
	    	  System.out.println(lineText);
	    	  HashMap<String,String> movieMap = new  HashMap<String,String>();
	    	  String[] lineFull = lineText.toString().trim().split("\\s+");
String[] users=lineFull[0].trim().split("######");
	    	  if(lineFull.length>1)
{
	    	  context.write(new Text(users[0].trim()),new Text(users[1].trim()+"######"+users[2].trim()+"######"+lineFull[1].trim()));
}	              
	      //}
	      }
   }
	   public static class Reduce2 extends Reducer<Text ,  Text ,  Text ,  Text> {
		   @Override 
	      public void reduce( Text word,  Iterable<Text > counts,  Context context)
	         throws IOException,  InterruptedException {
		        // Configuration conf = context.getConfiguration();
TreeMap<String,String> book_rating=new TreeMap<>();	
ArrayList<String> allbooks=new ArrayList<>();		   
ArrayList<String> booklist=new ArrayList<>();
for(Text rating : counts)
{
String[] all=rating.toString().split("######");
allbooks.add(all[1]);
}

for(Text rating : counts)
{
String[] alla=rating.toString().split("######");
StringBuffer sb=new StringBuffer();
String[] books=alla[2].split(",");
for(int i=0;i<books.length-1;i=i+2)
{
if(!(allbooks.contains(books[i])))
{
if(!(booklist.contains(books[i])))
{
booklist.add(books[i]);
sb.append(books[i]+",");
}
}
}
book_rating.put(alla[0]+alla[1],sb.toString());
context.write(word,new Text(sb.toString())); 
} 
StringBuffer sbu=new StringBuffer();
Collection c = book_rating.values();
   
    //obtain an Iterator for Collection
    Iterator itr = c.iterator();
   
    //iterate through TreeMap values iterator
    while(itr.hasNext())
{
      sbu.append(itr.next()+",");
}
//context.write(word,new Text(sbu.toString()));         

	      }





	   }


   	  
   }
