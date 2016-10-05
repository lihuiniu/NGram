import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class NGramLibraryBuilder {
	public static void main(String[] args) throws ClassNotFoundException{
		// TODO Auto-generated method stub

	}

	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		int noGram;
		
		@Override
		public void setup(Context context){
			Configuration conf = context.getConfiguration();
			//StringTokenizer st = new StringTokenizer(args)
			noGram = conf.getInt("noGram",5);//If no input, then use the default value 5.
		}
		
		@Override
		//key is the offset position in the file
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			//I love big datar
			String line = value.toString();
			line = line.trim().toLowerCase();
			line = line.replaceAll("[^a-z]", " ");
			String[] words = line.split("\\s+");
			if (words.length < 2){
				return;
			}
			StringBuilder sb;
			for (int i = 0; i < words.length - 1; i++){
				sb = new StringBuilder();
				sb.append(words[i]);
				for (int j = 1; j < noGram && i + j < words.length; j++){
					sb.append(" ");
					sb.append(words[i + j]);
					context.write(new Text(sb.toString().trim()), new IntWritable(1));
				}
			}
		}
	}
	public static class NGramReducer extends Reducer<Text, IntWritable,Text,IntWritable>{
			
			@Override
			public void reduce(Text key, Iterable<IntWritable> values,Context context)
										throws IOException, InterruptedException{
				int sum = 0;
				for (IntWritable value: values){
					sum += value.get();
				}
				context.write(key, new IntWritable(sum));
			}
	}
	
}
