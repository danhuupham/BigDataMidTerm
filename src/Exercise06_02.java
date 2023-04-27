import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class Exercise06_02 {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "Exercise06_02");
        job.setJarByClass(Exercise06_02.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileSystem fs = FileSystem.get(job.getConfiguration());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<Object, Text, IntWritable, Text> {
        int lineCount = 0;

        public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            context.write(new IntWritable(lineCount++), line);
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, Text, NullWritable> {
        public void reduce(IntWritable lineCount, Iterable<Text> rows, Context context) throws IOException, InterruptedException {
            ArrayList<ArrayList<Integer>> rowsNumber = new ArrayList<>(new ArrayList<>());
            for (Text row : rows) {
                StringTokenizer tokenizer = new StringTokenizer(row.toString());
                ArrayList<Integer> rowNumber = new ArrayList<>();
                while (tokenizer.hasMoreTokens()) {
                    rowNumber.add(Integer.parseInt(tokenizer.nextToken()));
                }
                rowsNumber.add(rowNumber);
            }
            ArrayList<Integer> sum = new ArrayList<>();
            for (int i = 0; i < rowsNumber.get(0).size(); i++) {
                int sumNumber = 0;
                for (ArrayList<Integer> rowNumber : rowsNumber) {
                    sumNumber += rowNumber.get(i);
                }
                sum.add(sumNumber);
            }
            StringBuilder sumString = new StringBuilder();
            for (int i = 0; i < sum.size(); i++) {
                sumString.append(sum.get(i));
                if (i != sum.size() - 1) {
                    sumString.append(" ");
                }
            }
            context.write(new Text(sumString.toString()), null);
        }
    }
}