package success;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@Log4j
public class ResultCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            log.error("사용법: ResultCount <input path> <output path>");
            System.exit(-1);
        }

        int exitCode = ToolRunner.run(new Configuration(), new ResultCount(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        conf.set("AppName", "Send Result");
        conf.set("resultCode", "200");

        String appName = conf.get("AppName");
        log.info("실행 중인 Job 이름: " + appName);

        Job job = Job.getInstance(conf);
        job.setJarByClass(ResultCount.class);
        job.setJobName(appName);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ResultCountMapper.class);
        job.setReducerClass(ResultCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
