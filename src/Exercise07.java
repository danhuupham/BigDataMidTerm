import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import java.util.StringTokenizer;

public class Exercise07 {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "Exercise07");
        job.setJarByClass(Exercise07.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

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

    public static class Map extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text line, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(line.toString());
            while (tokenizer.hasMoreTokens()) {
                context.write(new Text(tokenizer.nextToken()), key);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        Entry<String, Integer> maxEntry = null;

        public void reduce(Text word, Iterable<Text> ids, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> hashMap = new HashMap<>();
            for (Text id : ids) {
                String idString = id.toString();
                if (hashMap.containsKey(idString)) {
                    hashMap.put(idString, hashMap.get(idString) + 1);
                } else {
                    hashMap.put(idString, 1);
                }
            }
            maxEntry = null;
            for (Entry<String, Integer> entry : hashMap.entrySet()) {
                if (maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0) {
                    maxEntry = entry;
                }
            }
            context.write(word, new Text(maxEntry.getKey()));
        }
    }
}