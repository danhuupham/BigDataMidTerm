import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class Exercise03 {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "Exercise03");
        job.setJarByClass(Exercise03.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

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

    public static class Map extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(line.toString());
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class Combine extends Reducer<Text, IntWritable, Text, IntWritable> {
        int sum = 0;

        public void reduce(Text word, Iterable<IntWritable> count, Context context) throws IOException, InterruptedException {
            sum = 0;
            for (IntWritable value : count) {
                sum += value.get();
            }
            context.write(word, new IntWritable(sum));
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
        int max = 0;
        int min = 0;

        public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            max = Integer.MIN_VALUE;
            min = Integer.MAX_VALUE;
            for (IntWritable value : values) {
                if (value.get() > max) {
                    max = value.get();
                }
                if (value.get() < min) {
                    min = value.get();
                }
            }
            context.write(word, new Text(max + " " + min));
        }
    }
}