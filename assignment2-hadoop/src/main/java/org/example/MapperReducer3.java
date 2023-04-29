package org.example;

import java.io.IOException;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class MapperReducer3 {

    public static class MapperClass extends Mapper<Map3Key, DoubleWritable, Map3Key, DoubleWritable> {


        @Override
        public void map(Map3Key key, DoubleWritable value, Context context) throws IOException,  InterruptedException {
            context.write(key, value);
        }

        public void cleanup(Context context) throws IOException,  InterruptedException {
        }
    }

    public static class ReducerClass extends Reducer<Map3Key, DoubleWritable, Map3Key, DoubleWritable> {

        @Override
        public void reduce(Map3Key key, Iterable<DoubleWritable> values, Context context) throws IOException,  InterruptedException {
            for (DoubleWritable value : values) {
                context.write(key, value);
            }
        }

        public void cleanup(Context context) throws IOException,  InterruptedException {
        }

    }


    public static class PartitionerClass extends Partitioner<Map3Key, DoubleWritable> {
        @Override
        public int getPartition(Map3Key key, DoubleWritable value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "make output");
        job.setJarByClass(MapperReducer3.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(HashPartitioner.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Map3Key.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Map3Key.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }



}
