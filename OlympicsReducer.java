package session6.assignment1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OlympicsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int totalMedals = 0;
		for (IntWritable value : values) {
			totalMedals = totalMedals + value.get();
		}

		context.write(key, new IntWritable(totalMedals));
	}

}
