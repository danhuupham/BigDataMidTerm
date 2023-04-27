import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

public class Exercise03_02 {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "Exercise03_02");
        job.setJarByClass(Exercise03_02.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileSystem fs = FileSystem.get(job.getConfiguration());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(line.toString());
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();
                context.write(new Text(word), new Text(((FileSplit) context.getInputSplit()).getPath().getName()));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        int max = 0;
        int min = 0;

        public void reduce(Text word, Iterable<Text> filenames, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> hashMap = new HashMap<>();
            for (Text value : filenames) {
                String fileName = value.toString();
                if (hashMap.containsKey(fileName)) {
                    hashMap.put(fileName, hashMap.get(fileName) + 1);
                } else {
                    hashMap.put(fileName, 1);
                }
            }

            max = Integer.MIN_VALUE;
            min = Integer.MAX_VALUE;
            for (Integer value : hashMap.values()) {
                max = Math.max(max, value);
                min = Math.min(min, value);
            }
            context.write(word, new Text(max + " " + min));
        }
    }
}