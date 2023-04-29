package org.example;

import java.io.IOException;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class MapperReducer2 {

    public static class MapperClass extends Mapper<Text, Map1Value, Map2Key, Map2Value> {


        @Override
        public void map(Text key, Map1Value value, Context context) throws IOException,  InterruptedException {
            long corpusZeroCount = value.getCorpusZeroCount();
            long corpusOneCount = value.getCorpusOneCount();
            long totalCount = corpusZeroCount + corpusOneCount;
            String threeGram = key.toString();

            //            new key and value
//            key - r , boolean r_word == r , value {Nr_0, Nr_1 , Tr_01, Tr_10
            if(corpusZeroCount > 0){
                context.write(new Map2Key(corpusZeroCount, false), new Map2Value(threeGram, 1 , 0, corpusOneCount,0));
            }
            if(corpusOneCount > 0) {
                context.write(new Map2Key(corpusOneCount, false), new Map2Value(threeGram, 0, 1, 0, corpusZeroCount));
            }
             context.write(new Map2Key(totalCount, true), new Map2Value(threeGram, 0 , 0, 0, 0));
        }

        public void cleanup(Context context) throws IOException,  InterruptedException {
        }
    }

    public static class CombinerClass extends Reducer<Map2Key, Map2Value, Map2Key, Map2Value> {
        static long nr0 = 0;
        static long nr1 = 0;
        static long tr01 = 0;
        static long tr10 = 0;
        static long r = 0;


        @Override
        public void reduce(Map2Key key, Iterable<Map2Value> values, Context context) throws IOException, InterruptedException {
            long current_r = key.getR_value();
            if (r != current_r) {
                r = current_r;
                nr0 = 0;
                nr1 = 0;
                tr01 = 0;
                tr10 = 0;
            }

            boolean isThreeGram = key.getIsThreeGram();

            if (!isThreeGram) {
                for (Map2Value value : values) {
                    nr0 += value.getNr_0();
                    nr1 += value.getNr_1();
                    tr01 += value.getTr_01();
                    tr10 += value.getTr_10();
                }
                context.write(key, new Map2Value("", nr0, nr1, tr01, tr10));
            } else {
                for (Map2Value value : values) {
                    context.write(key, value);
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Map2Key, Map2Value, Map3Key, DoubleWritable> {
        static long nr0 = 0;
        static long nr1 = 0;
        static long tr01 = 0;
        static long tr10 = 0;
        static long r = 0;


        @Override
        public void reduce(Map2Key key, Iterable<Map2Value> values, Context context) throws IOException,  InterruptedException {
            final long N =  163471963L;

            long current_r = key.getR_value();
            if (r != current_r) {
                r = current_r;
                nr0 = 0;
                nr1 = 0;
                tr01 = 0;
                tr10 = 0;
            }

            boolean probabilityCheck = false;
            double probability = 0.0;

            boolean isThreeGram = key.getIsThreeGram();

            for (Map2Value value : values) {
                if (!isThreeGram) {
                    nr0 += value.getNr_0();
                    nr1 += value.getNr_1();
                    tr01 += value.getTr_01();
                    tr10 += value.getTr_10();
//                    System.out.println("ThreeGram: " + value.getThreeGram());
//                    System.out.println("nr0 : " + value.getNr_0() + "   , nr1 : " + value.getNr_1() + "    , tr01: " + value.getTr_01() + "     tr10: " + value.getTr_10());
//                    System.out.println("\nr0 : " + nr0 + "   , nr1 : " + nr1 + "    , tr01: " + tr01 + "     tr10: " + tr10 + "\n\n\nS");

                } else {
                    if(!probabilityCheck) {
                        probability = calculateProbability(nr0, nr1, tr01, tr10, N);
                        probabilityCheck = true;
                    }

                    String threeGram = value.getThreeGram();

                    Map3Key newKey = new Map3Key(threeGram, probability);
                    context.write(newKey, new DoubleWritable(probability));
                }
            }
        }

        public double calculateProbability(long Nr_0, long Nr_1, long Tr_01, long Tr_10, long N) {
            double denominator = N * (Nr_0 + Nr_1);
            if (denominator == 0.0){
                return 0.0;
            }

            double numerator = Tr_01 + Tr_10;

            return (numerator / denominator);
        }

        public void cleanup(Context context) throws IOException,  InterruptedException {
        }

    }



    public static class PartitionerClass extends Partitioner<Map2Key, Map2Value> {
        @Override
        public int getPartition(Map2Key key, Map2Value value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "neg dif");
        job.setJarByClass(MapperReducer2.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(HashPartitioner.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Map2Key.class);
        job.setMapOutputValueClass(Map2Value.class);
        job.setOutputKeyClass(Map3Key.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }



}
