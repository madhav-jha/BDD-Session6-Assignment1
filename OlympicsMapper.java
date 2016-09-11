package session6.assignment1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class OlympicsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("\t");

		if (lineArray[5].equals("Swimming")) {

			Text country = new Text(lineArray[2]);
			IntWritable medals = new IntWritable(Integer.parseInt(lineArray[9]));

			context.write(country, medals);

		}

	}

}
