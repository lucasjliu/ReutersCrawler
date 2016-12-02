import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

public class BuildIndex{

  private StopWordCollections stopwords;
  
  public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text wordText = new Text();
	private Text document = new Text();

	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		
		String line = value.toString();
		line.replace("| Reuters", "");//remove noise
		String[] doc = line.split("|");//split document id 
		String docid = doc[0];			//and content

		String[] titleArray = doc[1].split("[\\p{Punct}\\s]+");
		for(int i = 0; i <  titleArray.length; i++) { 
			if (stopwords.isStopWord(titleArray[i])) continue;
			wordText.set(titleArray[i]);
			document.set(docid + " Title " + Integer.toString(i));
			context.write(wordText,document);
		}

		String[] bodyArray = doc[2].split("[\\p{Punct}\\s]+");
		for(int i = 0; i <  bodyArray.length; i++) { 
			if (stopwords.isStopWord(bodyArray[i])) continue;
			wordText.set(bodyArray[i]);
			document.set(docid + " Body " + Integer.toString(i + titleArray.length);
			context.write(wordText,document);
		}
	}
  }
  
  public static class InvertedIndexReducer extends
Reducer<Text, Text, Text, Text> {
	protected void reduce(Text key, Iterable<Text> values, Context context) 
			throws java.io.IOException, InterruptedException {
		StringBuffer buffer = new StringBuffer();
		for (Text value : values) {
			if(buffer.length() != 0) {
				buffer.append(" ");
			}
			buffer.append(value.toString());
		}
		Text documentList = new Text();
		documentList.set(buffer.toString());
		context.write(key, documentList);
	}
  } 

  public static void main(String[] args) 
		  throws IOException, ClassNotFoundException, InterruptedException {
    if (args.length != 2) {
      System.err.println("Usage: InvertedIndex <input path> <output path>");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "build index");
    job.setJarByClass(BuildIndex.class);
   
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    stopwords = new StopWordCollections();

    job.setMapperClass(InvertedIndexMapper.class);
    //job.setCombinerClass(InvertedIndexReducer.class);
    job.setReducerClass(InvertedIndexReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static  class StopWordCollections {
	private List<String> stopWordList;
	
	public StopWordCollections() throws IOException {
		String[] mystop = {"a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these", "they", "this", "to", "was", "will", "with"}
		List<String> stopWordList = new ArrayList<String>();
		for (String str : mystop) {
			stopWordList.add(str);
		}
	}
	
	public Boolean isStopWord(String word) {
		return stopWordList.contains(word);
	}
  }
}
