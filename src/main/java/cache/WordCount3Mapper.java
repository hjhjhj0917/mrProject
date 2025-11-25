package cache;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class WordCount3Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Set<String> wordSet = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException {

        URI[] cacheFiles = context.getCacheFiles();

        if (cacheFiles != null) {
            for (URI cacheFile : cacheFiles) {
                String fileName = new Path(cacheFile.getPath()).getName();

                BufferedReader reader = new BufferedReader(new FileReader(fileName));
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim().toLowerCase();

                    if (!line.isEmpty()) {
                        wordSet.add(line);
                    }
                }
                reader.close();
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        for (String word : line.split("\\W+")) {
            if (!word.isEmpty()) {
                String lowerWord = word.toLowerCase();

                if (wordSet.contains(lowerWord)) {
                    context.write(new Text(lowerWord), new IntWritable(1));
                }
            }

        }
    }
}
