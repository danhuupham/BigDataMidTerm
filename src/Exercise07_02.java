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
import java.util.ArrayList;
import java.util.StringTokenizer;

public class Exercise07_02 {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "Exercise07_02");
        job.setJarByClass(Exercise07_02.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileSystem fs = FileSystem.get(job.getConfiguration());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<Object, Text, IntWritable, Text> {
        int row = 0;
        int column = 0;

        public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(line.toString());
            column = 0;
            while (tokenizer.hasMoreTokens()) {
                context.write(new IntWritable(column), new Text(row + " " + tokenizer.nextToken()));
                column++;
            }
            row++;
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable col, Iterable<Text> rowAndValues, Context context) throws IOException, InterruptedException {
            ArrayList<Integer> multiply = new ArrayList<>();
            for (Text rowAndValue : rowAndValues) {
                StringTokenizer tokenizer = new StringTokenizer(rowAndValue.toString());
                int row = Integer.parseInt(tokenizer.nextToken());
                int value = Integer.parseInt(tokenizer.nextToken());
                if (multiply.size() <= row) {
                    for (int i = multiply.size(); i <= row; i++) {
                        multiply.add(1);
                    }
                }
                multiply.set(row, multiply.get(row) * value);
            }

            StringBuilder multiplyString = new StringBuilder();
            for (Integer integer : multiply) {
                multiplyString.append(integer).append(" ");
            }
            context.write(col, new Text(multiplyString.toString()));
        }
    }
}