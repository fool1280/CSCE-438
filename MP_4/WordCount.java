import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class WordCount {

    public static class TweetMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        int hour;

        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("T")) {
                String[] token = line.trim().split(" ");
                String[] timestamp = token[1].split(":");
                hour = Integer.parseInt(timestamp[0]);
                // for time of day most of the tweets
                // context.write(new IntWritable(hour), new IntWritable(1));
            } else if (line.startsWith("W")) {
                String[] content = line.trim().split(" ");
                for (String word : content) {
                    if (word.equals("sleep")) {
                        context.write(new IntWritable(hour), new IntWritable(1));
                        break;
                    }
                }
            }
        }
    }

    public static class TweetReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Read tweets line by line");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TweetMapper.class);
        job.setReducerClass(TweetReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}