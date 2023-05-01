// Part 1:

// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.mapreduce.Job;
// import org.apache.hadoop.mapreduce.Mapper;
// import org.apache.hadoop.mapreduce.Reducer;
// import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
// import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// import java.io.IOException;

// public class WordCount {

// public static class TweetMapper extends Mapper<LongWritable, Text,
// IntWritable, IntWritable> {
// int hour;

// protected void map(LongWritable key, Text value, Context context)
// throws IOException, InterruptedException {
// String line = value.toString();
// if (line.startsWith("T")) {
// String[] token = line.trim().split(" ");
// String[] timestamp = token[1].split(":");
// hour = Integer.parseInt(timestamp[0]);
// context.write(new IntWritable(hour), new IntWritable(1));
// }
// }
// }

// public static class TweetReducer extends Reducer<IntWritable, IntWritable,
// IntWritable, IntWritable> {
// protected void reduce(IntWritable key, Iterable<IntWritable> values, Context
// context)
// throws IOException, InterruptedException {
// int sum = 0;
// for (IntWritable value : values) {
// sum += value.get();
// }
// context.write(key, new IntWritable(sum));
// }
// }

// public static void main(String[] args) throws Exception {

// Configuration conf = new Configuration();
// Job job = Job.getInstance(conf, "Read tweets line by line");
// job.setJarByClass(WordCount.class);
// job.setMapperClass(TweetMapper.class);
// job.setReducerClass(TweetReducer.class);

// job.setInputFormatClass(TextInputFormat.class);
// job.setOutputFormatClass(TextOutputFormat.class);

// job.setOutputKeyClass(IntWritable.class);
// job.setOutputValueClass(IntWritable.class);

// FileInputFormat.addInputPath(job, new Path(args[0]));
// FileOutputFormat.setOutputPath(job, new Path(args[1]));

// System.exit(job.waitForCompletion(true) ? 0 : 1);
// }
// }

// Part 2: Too slow
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Mapper;
// import org.apache.hadoop.mapreduce.Reducer;
// import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.conf.Configured;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.mapreduce.Job;
// import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
// import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
// import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
// import org.apache.hadoop.util.Tool;
// import org.apache.hadoop.util.ToolRunner;

// import java.io.IOException;

// public class WordCount extends Configured implements Tool {

// public static class TweetMapper extends Mapper<LongWritable, Text,
// IntWritable, IntWritable> {
// int hour;

// protected void map(LongWritable key, Text value, Context context)
// throws IOException, InterruptedException {
// String line = value.toString();
// if (line.startsWith("T")) {
// String[] token = line.trim().split(" ");
// String[] timestamp = token[1].split(":");
// hour = Integer.parseInt(timestamp[0]);
// // for time of day most of the tweets
// // context.write(new IntWritable(hour), new IntWritable(1));
// } else if (line.startsWith("W")) {
// String[] content = line.trim().split(" ");
// for (String word : content) {
// if (word.toLowerCase().contains("sleep")) {
// context.write(new IntWritable(hour), new IntWritable(1));
// break;
// }
// }
// }
// }
// }

// public static class TweetReducer extends Reducer<IntWritable, IntWritable,
// IntWritable, IntWritable> {
// protected void reduce(IntWritable key, Iterable<IntWritable> values, Context
// context)
// throws IOException, InterruptedException {
// int sum = 0;
// for (IntWritable value : values) {
// sum += value.get();
// }
// context.write(key, new IntWritable(sum));
// }
// }

// @Override
// public int run(String[] args) throws Exception {

// Job job = new Job(getConf());
// job.setJobName("NLineInputFormat example");
// job.setJarByClass(WordCount.class);

// job.setInputFormatClass(NLineInputFormat.class);
// NLineInputFormat.addInputPath(job, new Path(args[0]));
// job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap",
// 5000);

// LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
// FileOutputFormat.setOutputPath(job, new Path(args[1]));
// job.setOutputKeyClass(IntWritable.class);
// job.setOutputValueClass(IntWritable.class);

// job.setMapperClass(TweetMapper.class);
// job.setReducerClass(TweetReducer.class);

// boolean success = job.waitForCompletion(true);
// return success ? 0 : 1;
// }

// public static void main(String[] args) throws Exception {
// int exitCode = ToolRunner.run(new Configuration(), new WordCount(), args);
// System.exit(exitCode);
// }
// }

// Part 2: 2nd try
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
                    if (word.toLowerCase().contains("sleep")) {
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