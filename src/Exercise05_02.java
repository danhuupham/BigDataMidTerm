import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.StringTokenizer;

public class Exercise05_02 {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "Exercise05_02");
        job.setJarByClass(Exercise05_02.class);

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
        private String id = "";
        private String firstWord = "";

        public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(line.toString());

            if (tokenizer.hasMoreTokens()) {
                firstWord = tokenizer.nextToken();
            }

            if (!tokenizer.hasMoreTokens()) {
                id = firstWord;
            }

            while (tokenizer.hasMoreTokens()) {
                String secondWord = tokenizer.nextToken();
                context.write(new Text(firstWord + " " + secondWord), new Text(id));
                firstWord = secondWord;
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        Entry<String, Integer> maxEntry = null;

        public void reduce(Text word, Iterable<Text> ids, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> idCount = new HashMap<>();
            for (Text id : ids) {
                String idString = id.toString();
                if (idCount.containsKey(idString)) {
                    idCount.put(idString, idCount.get(idString) + 1);
                } else {
                    idCount.put(idString, 1);
                }
            }

            maxEntry = null;
            for (Entry<String, Integer> entry : idCount.entrySet()) {
                if (maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0) {
                    maxEntry = entry;
                }
            }
            context.write(word, new Text(maxEntry.getKey()));
        }
    }
}