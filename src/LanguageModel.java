import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import java.util.List;
import java.util.TreeMap;
public class LanguageModel {

	public LanguageModel() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
public static class LanguageMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		int noGram;
		int threshold;
		
		@Override
		public void setup(Context context){
			Configuration conf = context.getConfiguration();
			//StringTokenizer st = new StringTokenizer(args)
			threshold = conf.getInt("threshold",20);//If no input, then use the default value 20.
		}
		
		@Override
		//key is the offset position in the file
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			//I love big datar
			if (value == null){
				return ;
			}
			if (value.toString().trim().length() == 0){
				return ;
			}
			//this is cool\t20
			String line = value.toString().trim();
			String[] wordsPlusCount = line.split("\t");
			if (wordsPlusCount.length < 2){
				return ;
			}
			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);
			if (count < threshold){
				return ;
			}
			//key: this is
			//value: cool = 20
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < words.length - 1; i++){
				sb.append(words[i]).append(" ");
			}
			String outputKey = sb.toString().trim();
			String outputValue = words[words.length - 1];
			if (outputKey == null || outputKey.length() < 1){
				return ;
			}
			context.write(new Text(outputKey), new Text(outputValue + "=" + count));
		}
	}
	public static class LanguageReducer extends Reducer<Text, Text,DBOutputWritable,NullWritable>{
		
		int n;
		
		@Override
		public void setup(Context context){
			Configuration conf = context.getConfiguration();
			//StringTokenizer st = new StringTokenizer(args)
			n = conf.getInt("n",5);//If no input, then use the default value 5.
		}	
			
		@Override
		public void reduce(Text key, Iterable<Text> values,Context context)
										throws IOException, InterruptedException{
			//this is, <girl = 50, boy = 60?
			TreeMap<Integer, List<String>> tm = new TreeMap<>(Collections.reverseOrder());
			for (Text val : values){
				String curValue = val.toString();
				String word = curValue.split("=")[0].trim();
				int count = Integer.parseInt(curValue.split("=")[1].trim());
				if (tm.containsKey(count)){
					tm.get(count).add(word);
				} else {
					List<String> list = new ArrayList<>();
					list.add(word);
					tm.put(count, list);
				}
			}
			Iterator<Integer> iter =tm.keySet().iterator();
			for (int j = 0; j <= n && iter.hasNext(); j++){
				int keyCount =iter.next();
				List<String> words =tm.get(keyCount);
				for (String curWord: words){
					context.write
				}
			}
		}
	}

}
