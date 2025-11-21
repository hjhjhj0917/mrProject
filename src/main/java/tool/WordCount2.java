package tool;

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

public class WordCount2 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("사용법: WordCount2 <input path> <output path>");
            System.exit(-1);
        }

        int exitCode = ToolRunner.run(new Configuration(), new WordCount2(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        conf.set("AppName", "ToolRunner Test");

        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount2.class);
        job.setJobName(conf.get("AppName"));

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(WordCount2Mapper.class);
        job.setReducerClass(WordCount2Reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
