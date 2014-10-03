import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class MaxTemp extends Configured implements Tool {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		private Text year = new Text();
		private IntWritable temp = new IntWritable();

		public void configure(JobConf job) {
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();

			String thisYear = line.substring(15,19);
			String sign = line.substring(87,88);
			int thisTemp = Integer.parseInt(line.substring(88,92));
			int quality = Integer.parseInt(line.substring(92,93));

			if(sign.equals("-")){
				thisTemp = -1*thisTemp;}

			if(thisTemp != 9999 && (quality == 0 || quality == 1 || quality == 4 || quality == 5 || quality == 9)){
				year.set(thisYear);
				temp.set(thisTemp);
				output.collect(year, temp);	
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int max = -1000;
			int thisTemp = 0;
			while(values.hasNext()){
				thisTemp = values.next().get();
				if(thisTemp > max)
					max = thisTemp;
			}
			output.collect(key, new IntWritable(max));
		}
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), MaxTemp.class);
		conf.setJobName("temperature");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		return 0;
    }

    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MaxTemp(), args);
		System.exit(res);
    }
}
