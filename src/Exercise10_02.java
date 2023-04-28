import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Exercise10_02 {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "Exercise10_02");
        job.setJarByClass(Exercise10_02.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.getConfiguration().set("param", args[2]);

        FileSystem fs = FileSystem.get(job.getConfiguration());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<Text, Text, IntWritable, Text> {
        public void map(Text point, Text coordinate, Context context) throws IOException, InterruptedException {
            int query = Integer.parseInt(context.getConfiguration().get("param"));
            context.write(new IntWritable(Math.abs(query - Integer.parseInt(coordinate.toString()))), point);
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable distance, Iterable<Text> points, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text point : points) {
                sb.append(point.toString()).append(" ");
            }
            context.write(distance, new Text(sb.toString()));
        }
    }
}