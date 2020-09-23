import java.io.IOException;
import java.util.StringTokenizer;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
 
public class azm5MapReduce extends Configured implements Tool{

	@Override
    
public int run(String[] args) throws Exception 
    {
        Job checkdeck = Job.getInstance(getConf(), "MissingCards");       
        checkdeck.setJarByClass(getClass());
        TextInputFormat.addInputPath(checkdeck, new Path(args[0]));
        checkdeck.setInputFormatClass(TextInputFormat.class);
        checkdeck.setMapperClass(Map.class);
        checkdeck.setReducerClass(Reduce.class);
        TextOutputFormat.setOutputPath(checkdeck, new Path(args[1]));
        checkdeck.setMapOutputKeyClass(Text.class);
        checkdeck.setMapOutputValueClass(IntWritable.class);
        checkdeck.setOutputFormatClass(TextOutputFormat.class);
        checkdeck.setOutputKeyClass(Text.class);
        checkdeck.setOutputValueClass(Text.class);
        return checkdeck.waitForCompletion(true)?0 : 1;
    }

public static void main(String[] args) throws Exception 
    {
        int end = ToolRunner.run(new azm5MapReduce(), args);
        System.exit(end);
    }
}

class Map extends Mapper<LongWritable, Text, Text, IntWritable>
{
    Text t1 = new Text();
    Text t2 = new Text();

public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String line = value.toString();
        String[] field = line.split(",");
        t2.set(field[0]);
        t1.set(field[1]);
        context.write(t2,new IntWritable(Integer.parseInt(field[1])));
    }

}

class Reduce extends Reducer<Text, IntWritable, Text, Text>

{
    Text Deck = new Text();    
    public void reduce(Text token, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException
    {
        ArrayList<Integer> a = new ArrayList<Integer>();
        int sum=0;
        int temporary=0;
        for (IntWritable value : counts)
        {
            sum= sum+value.get();
            temporary=value.get();
            a.add(temporary);
        }

        StringBuilder stringBuilder = new StringBuilder();
        if(sum<91)
        {
        for (int i=1;i<=13;i++)
        {
            if(!a.contains(i))
            stringBuilder.append(i).append("   ");
        }
        
    Deck.set(stringBuilder.substring(0,stringBuilder.length()-1));   
    }
        
else
    {
        Deck.set("No Cards missing in this suite");
    }

context.write(token, Deck);

    }

}