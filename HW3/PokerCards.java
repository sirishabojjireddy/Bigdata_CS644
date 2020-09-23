import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;


class PokerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	public void reduce(Text key, Iterable<IntWritable> values, Context c)throws IOException, InterruptedException
	{
		int i = 1,cardPresent = 0;
		ArrayList<Integer> poker = new ArrayList<Integer>();

		for(i=1;i<=13;++i)
		{
			poker.add(i);
		}

		for(IntWritable cards : values)
		{
			cardPresent = cards.get();
			if(poker.contains(cardPresent))
			{
				poker.remove(poker.indexOf(cardPresent));
			}
		}
		for(i=0;i<poker.size();++i)
		{
			c.write(key, new IntWritable(poker.get(i)));
		}

	}
}

public class PokerCards
{
	/*
	*Main Function which is also a driver function for Map Reduce Program
	*/
	public static void main(String[] args)throws Exception
	{
		Configuration config = new Configuration();
		Job job = new Job(config, "PokerCards");

		job.setJarByClass(PokerCards.class);

		job.setMapperClass(PokerMapper.class);
		job.setReducerClass(PokerReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}


class PokerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	public void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException
	{
		String str = value.toString();
		String[] split = str.split("	");
                System.out.println("Split "+split); 
                System.out.println("str "+str); 
                // System.out.println(split[0] + ":"+ split[1]); 
		Text text = new Text(split[0]);
		IntWritable in = new IntWritable(Integer.parseInt(split[1]));
		c.write(text,in);
	}
}


