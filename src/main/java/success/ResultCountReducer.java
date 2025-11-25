package success;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ResultCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int resultCodeCount = 0;

        for (IntWritable value : values) {
            resultCodeCount += value.get();
        }

        context.write(key, new IntWritable(resultCodeCount));
    }
}
