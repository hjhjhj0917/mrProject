package success;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

@Log4j
public class ResultCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    String appName = "";
    String resultCode = "";

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        super.setup(context);

        Configuration conf = context.getConfiguration();

        this.appName = conf.get("AppName");
        this.resultCode = conf.get("resultCode", "200");

        log.info("[" + this.appName + "] 난 map함수 실행하기 전에 1번만 실행되는 setup함수다!");
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        super.cleanup(context);

        log.info("[" + this.appName + "] 난 에러나도 무조건 실행되는 cleanup함수다!");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] arr = line.split("\\W+");

        int pos = arr.length - 2;

        String result = arr[pos];

        log.info("[" + this.appName + "]" + result);

        if (resultCode.equals(result)) {
            context.write(new Text(result), new IntWritable(1));
        }
    }
}
