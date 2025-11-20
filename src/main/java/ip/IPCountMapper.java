package ip;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IPCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString().trim();

        String[] data = line.split(" ");

        if (data.length > 3 && data[0].matches("\\d+\\.\\d+\\.\\d+\\.\\d+")) {
            String ip = data[0];
            context.write(new Text(ip), new IntWritable(1));
        }
    }
}
