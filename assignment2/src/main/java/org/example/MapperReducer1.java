package org.example;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class MapperReducer1 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Map1Value> {
        private boolean part = false;
        HashSet<String> stopWords = getStopWordsAsSet();


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            //TODO get the value correct from aws
            long corpusZeroOccurrences = 0;
            long corpusOneOccurrences = 0;
            String [] splitToken = value.toString().split("\\t");
            if(splitToken.length == 0) {
            }
            else {
                String threeGram = splitToken[0];
                if (isThreeGramValid(threeGram, stopWords)) {
                    Text newKey_3Gram = new Text(threeGram);
                    long occurrences = Integer.parseInt(splitToken[2]);
                    //get part 0 or 1
                    if (!part) {
                        corpusZeroOccurrences = occurrences;
                    } else {
                        corpusOneOccurrences = occurrences;
                    }
                    part = !part;
                    //make the DataGramAndCount
                    Map1Value newValue = new Map1Value(corpusZeroOccurrences, corpusOneOccurrences);
                    context.write(newKey_3Gram, newValue);
                }
            }
        }

        public HashSet<String> getStopWordsAsSet() {
            HashSet<String> stopWords= new HashSet<>();
            Region region = Region.US_EAST_1;

            //s3
            S3Client s3 = S3Client.builder()
                    .region(region)
                    .build();

            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket("ofiwjoiwf")
                    .key("heb-stopwords.txt")
                    .build();

            ResponseInputStream<GetObjectResponse> input = s3.getObject(getObjectRequest);
            BufferedReader reader = new BufferedReader(new InputStreamReader(input,StandardCharsets.UTF_8));
            while (true) {
                String line;
                try {
                    line = reader.readLine();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if (line == null)
                    break;
                stopWords.add(line);
            }
            return stopWords;
        }


        public boolean isThreeGramValid(String threeGram, HashSet<String> stopWords) {
            String [] threeGramAsArray = threeGram.split("\\s+");

            //check we got threegram and not grabage
            if (threeGramAsArray.length != 3) {
                return false;
            }

            for (String word : threeGramAsArray) {
                if (stopWords.contains(word)) {
                    return false;
                }
            }
            return true;
        }

        public void cleanup(Context context) {
        }
    }

    public static class ReducerClass extends Reducer<Text, Map1Value,Text, Map1Value> {
        @Override
        public void reduce(Text key, Iterable<Map1Value> values, Context context) throws IOException,  InterruptedException {

            //check how to create 1 key for the total count
            long corpusZeroCount = 0;
            long corpusOneCount = 0;

            for (Map1Value value : values) {
                corpusZeroCount += value.getCorpusZeroCount();
                corpusOneCount += value.getCorpusOneCount();
            }

            Map1Value dataGram = new Map1Value(corpusZeroCount, corpusOneCount);

            context.write(key, dataGram);
        }

        public void cleanup(Context context) {
        }
    }



    public static class PartitionerClass extends Partitioner<Text, Map1Value> {
        @Override
        public int getPartition(Text key, Map1Value value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "3gram count");
        job.setJarByClass(MapperReducer1.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(HashPartitioner.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Map1Value.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Map1Value.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
