import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.IOException;

public class WordCount {

    public static class NewInputFormat extends FileInputFormat<Object, Text> {
        @Override
        public RecordReader<Object, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            return new TweetRecordReader();
        }
    }

    public static class TweetRecordReader extends RecordReader<Object, Text> {
        LineRecordReader currentLine;
        Object currentKey;
        Text currentValue;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            currentLine = new LineRecordReader();
            currentLine.initialize(split, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            String continuing_line = new String();
            for (int i = 0; i < 4; i++) {
                if (!currentLine.nextKeyValue()) {
                    return false;
                }
                continuing_line += currentLine.getCurrentValue().toString() + "\n";
            }
            Text val = new Text();
            val.set(continuing_line);
            currentValue = new Text(val);
            return true;
        }

        @Override
        public Object getCurrentKey() throws IOException, InterruptedException {
            return currentKey;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return currentValue;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return currentLine.getProgress();
        }

        @Override
        public void close() throws IOException {
            currentLine.close();
        }
    }

    public static class TweetMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        int hour;

        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] lines = value.toString().split("\n");
            for (String line : lines) {
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

        job.setInputFormatClass(NewInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}