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

public class BookCountryCosine extends Configured implements Tool {

	
   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new BookCountryCosine(), args);
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
      if(jobmov2.waitForCompletion(true)){
    	  jobmov3 = Job .getInstance(getConf(), " MovieData ");
    	  jobmov3.setJarByClass(this .getClass());
    	 //job2.getConfiguration().set("nOD", String.valueOf(cs.getFileCount()));
    	 //job2.getConfiguration().set("tmp_out", tmp_out);
    	  jobmov3.setMapperClass( Map3 .class);
    	  jobmov3.setReducerClass( Reduce3 .class);
    	  jobmov3.setOutputKeyClass( Text .class);
    	  jobmov3.setOutputValueClass( Text .class);
    	  FileInputFormat.addInputPaths(jobmov3,  args[1]+"/out/");
          FileOutputFormat.setOutputPath(jobmov3,  new Path(args[1]+"/out/output"));
      }

      }

      return jobmov3.waitForCompletion( true)  ? 0 : 1;
   }
   
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
  context.write(new Text(country),new Text(rating[0]+"######"+rating[1]));

}
} 
}
}
}
} 
    }

   }

   public static class Reduce extends Reducer<Text ,  Text ,  Text ,  Text > {
      @Override 
      public void reduce( Text word,  Iterable<Text > counts,  Context context)
         throws IOException,  InterruptedException {
    	  
    	  StringBuffer sb  = new StringBuffer();
         for ( Text rating  : counts) {
             sb.append(rating.toString());
sb.append("######");

        	 } 

        context.write(word,new Text(sb.toString()));
         
      }
   }
   


//Mapper output key is country and value is the book id and rating given by each user in that age

   
   public static class Map2 extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
	      

	      public void map( LongWritable offset,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {
	       	  
	    	  System.out.println(lineText);
	    	  HashMap<String,String> movieMap = new  HashMap<String,String>();
	    	  String[] lineFull = lineText.toString().split("\\s+");
String[] country=lineFull[0].toString().split("######");
	    	  String[] line  = lineFull[1].toString().split("######");
	    	  int len_arr = 0;
double value=0.0;
		    	  for(int i=0;i<line.length-1;i=i+2){
if(movieMap.containsKey(line[i]))
{
value=(Double.parseDouble(line[i+1])+Double.parseDouble(movieMap.get(line[i])))/2;
	    		  movieMap.put(line[i], value+"");
}
else
{
		    		  movieMap.put(line[i], line[i+1]);
}		    		  
		    		  
		    	  }
		          ArrayList<String> array_movie = new ArrayList<String>( movieMap.keySet()); 
		          len_arr = array_movie.size();
		    	  for(int j=0;j<len_arr;j++){
		    		  for(int i=j+1;i<len_arr;i++){
		    			  
		    			  context.write(new Text(lineFull[0]+","+array_movie.get(j)+","+array_movie.get(i)),
		    					  new Text(movieMap.get(array_movie.get(j))+","+movieMap.get(array_movie.get(i))));
		    			  
		    		  }
		    	  }
	    	  
	    	  
	              
	      //}
	      }
   }
//reduce output key is country and value is all the book id and corresponding rating given by each user in that age
	   public static class Reduce2 extends Reducer<Text ,  Text ,  Text ,  Text> {
		   @Override 
	      public void reduce( Text word,  Iterable<Text > counts,  Context context)
	         throws IOException,  InterruptedException {
		        // Configuration conf = context.getConfiguration();
			   try{
				   	  
	    	  DecimalFormat deciFormat = new DecimalFormat("#.0");
	    	  int xy=0;
	    	  int corr_xy=0;
	    	  int x2= 0;
	    	  int y2= 0;
	    	  int x2_corr= 0;
	    	  int y2_corr= 0;
	    	  int mean_x = 0;
	    	  int mean_y=0;
	    	  double corr = 0;
	    	  double cosine = 0;
	    	  double jaccard_coeff = 0;
	    	  int num_jac = 0;
	    	  int deno_jac =0;
	    	  String[] movieId = word.toString().split(",");
	    	  String[] ratings=null;
	    	  ArrayList<Integer> al = new ArrayList<Integer>();
	    	  for ( Text count  : counts) {
	    		  ratings= count.toString().split(",");
	    		  al.add(Integer.parseInt(ratings[0]));
	    		  al.add(Integer.parseInt(ratings[1]));
	    		  
	           }
	    	  int x =0;
	    	  int y =0;
	    	  int mean_count = al.size()/2;
	    	  for (int i=0;i<al.size();i=i+2) {
	    		  x=al.get(i);
	    		  y=al.get(i+1);
	    		  if(x>y){
	    			  num_jac = num_jac+y;
	    			  deno_jac = deno_jac+x;
	    		  }
	    		  else{
	    			  num_jac = num_jac+x;
	    			  deno_jac = deno_jac+y;
	    			  
	    		  }
	    		  mean_x = mean_x+x;
	    		  mean_y = mean_y+y;
	    		  
	    		  xy=xy+(x*y);
	    		  x2=x2+(x* x);
	    		  y2=y2+(y* y);
	    		   
	    	  }
	    	
	    	  mean_x = mean_x/mean_count;
	    	  mean_y = mean_y/mean_count;
	    	  	  cosine=(xy)/(Math.sqrt(x2) * Math.sqrt(y2));
	    	  for (int i=0;i<al.size();i=i+2) {
	    		  corr_xy=corr_xy+((al.get(i)-mean_x)*(al.get(i+1)-mean_y));
	    		  x2_corr=x2_corr+((al.get(i)-mean_x)* (al.get(i)-mean_x));
	    		  y2_corr=y2_corr+((al.get(i+1)-mean_y)* (al.get(i+1)-mean_y));
	    		   
	    	  }
	    	  System.out.println("corr_xy--> "+ corr_xy);
	    	  System.out.println("Math.sqrt((x2_corr*y2_corr))--> "+ Math.sqrt((x2_corr*y2_corr)));
	    	   corr = corr_xy/(Math.sqrt((x2_corr*y2_corr)));
	    	   //if(deno_jac!=0){
	    		//   jaccard_coeff = (num_jac/deno_jac) ;
	    	   //}
	    		   
//if((Double.isNaN(corr)))
//{
//corr=0.0;
//}

if(!(Double.isNaN(cosine)))
{			   	  System.out.println("Inside Reduce2 ---->>>jaccard_coeff, corr, cos"+jaccard_coeff+" "+corr+" "+cosine);
	    		  context.write(new Text(movieId[0]+","+movieId[1]+","+movieId[2]), new Text(deciFormat.format(cosine)));
	    		  context.write(new Text(movieId[0]+","+movieId[2]+","+movieId[1 ]), new Text(deciFormat.format(cosine)));
}
			   }
			   catch(InterruptedException Ie){
				   Ie.printStackTrace();
			   }
			   
	         
	      }
	   }

//Mapper output key is country and combination of all pair of books and the average of their ratings given in that age
   
   public static class Map3 extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
	      

	      public void map( LongWritable offset,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {
	       	  
	    	  System.out.println(lineText);
	    	  HashMap<String,String> movieMap = new  HashMap<String,String>();
String[] all=lineText.toString().split("\\s+");
	    	  String[] lineFull = all[0].split(",");
String[] values=all[1].split(",");
	    	    			  	    	    			  context.write(new Text(lineFull[0]+","+lineFull[1]),
		    					  new Text(lineFull[2].trim()+","+all[1]));

		  	    	  
	              
	      //}
	      }
   }

	   public static class Reduce3 extends Reducer<Text ,  Text ,  Text ,  Text> {
		   @Override 
	      public void reduce( Text word,  Iterable<Text > counts,  Context context)
	         throws IOException,  InterruptedException {
		        // Configuration conf = context.getConfiguration();
HashMap<String,Double> value=new HashMap<>();
			   
for(Text count:counts)
{

String[] line=count.toString().split(",");
value.put(line[0],Double.parseDouble(line[1]));

}
List<Entry<String, Double>> list =
                new LinkedList<Entry<String, Double>>(value.entrySet());

        // 2. Sort list with Collections.sort(), provide a custom Comparator
        //    Try switch the o1 o2 position for a different order
        Collections.sort(list, new Comparator<Entry<String, Double>>() {
            public int compare(Entry<String, Double> o1,
                               Entry<String, Double> o2) {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });

        // 3. Loop the sorted list and put it into a new insertion order Map LinkedHashMap
        HashMap<String, Double> sortedMap = new LinkedHashMap<String, Double>();
        for (Entry<String, Double> entry : list) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
StringBuffer sb=new StringBuffer();
for(String key:sortedMap.keySet())
{

sb.append(key+","+sortedMap.get(key)+",");
//ntext.write(new Text(word),new Text(all[0]+","+sortedMap.get(key)+","+all[1]+","+all[2]));

}
context.write(new Text(word),new Text(sb.toString()));
	   }
}
}
