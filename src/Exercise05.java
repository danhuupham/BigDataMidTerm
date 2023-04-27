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

public class Exercise05 {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "Exercise05");
        job.setJarByClass(Exercise05.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileSystem fs = FileSystem.get(job.getConfiguration());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(line.toString());
            while (tokenizer.hasMoreTokens()) {
                context.write(new Text(tokenizer.nextToken()), new IntWritable(1));
            }
        }
    }

    public static class Combine extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            context.write(word, new IntWritable(1));
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        int count = 0;

        public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            count = 0;
            for (IntWritable ignored : values) {
                count++;
            }
            context.write(word, new IntWritable(count));
        }
    }
}