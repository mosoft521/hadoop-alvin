package com.gmail.mosoft521.hadoop.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by Alvin on 2017/1/9 0009.
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
/*
打包为:hadoop-alvin-1.0-SNAPSHOT.jar
上传到hadoop任意节点
#hadoop jar hadoop-alvin-1.0-SNAPSHOT.jar /user/hadoop/input/words /user/hadoop/output
[root@master hadoop-2.7.3]# hadoop jar hadoop-alvin-1.0-SNAPSHOT.jar /user/hadoop/input/words /user/hadoop/output
17/01/09 12:18:17 INFO client.RMProxy: Connecting to ResourceManager at master/172.16.3.211:8032
17/01/09 12:18:19 INFO input.FileInputFormat: Total input paths to process : 1
17/01/09 12:18:19 WARN hdfs.DFSClient: Caught exception
java.lang.InterruptedException
        at java.lang.Object.wait(Native Method)
        at java.lang.Thread.join(Thread.java:1249)
        at java.lang.Thread.join(Thread.java:1323)
        at org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.closeResponder(DFSOutputStream.java:609)
        at org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.endBlock(DFSOutputStream.java:370)
        at org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.run(DFSOutputStream.java:546)
17/01/09 12:18:19 INFO mapreduce.JobSubmitter: number of splits:1
17/01/09 12:18:19 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1483933731208_0002
17/01/09 12:18:19 INFO impl.YarnClientImpl: Submitted application application_1483933731208_0002
17/01/09 12:18:19 INFO mapreduce.Job: The url to track the job: http://master:8088/proxy/application_1483933731208_0002/
17/01/09 12:18:19 INFO mapreduce.Job: Running job: job_1483933731208_0002
17/01/09 12:18:28 INFO mapreduce.Job: Job job_1483933731208_0002 running in uber mode : false
17/01/09 12:18:28 INFO mapreduce.Job:  map 0% reduce 0%
17/01/09 12:18:34 INFO mapreduce.Job:  map 100% reduce 0%
17/01/09 12:18:40 INFO mapreduce.Job:  map 100% reduce 100%
17/01/09 12:18:40 INFO mapreduce.Job: Job job_1483933731208_0002 completed successfully
17/01/09 12:18:41 INFO mapreduce.Job: Counters: 49
        File System Counters
                FILE: Number of bytes read=55
                FILE: Number of bytes written=237361
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=137
                HDFS: Number of bytes written=33
                HDFS: Number of read operations=6
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
        Job Counters
                Launched map tasks=1
                Launched reduce tasks=1
                Data-local map tasks=1
                Total time spent by all maps in occupied slots (ms)=3668
                Total time spent by all reduces in occupied slots (ms)=4308
                Total time spent by all map tasks (ms)=3668
                Total time spent by all reduce tasks (ms)=4308
                Total vcore-milliseconds taken by all map tasks=3668
                Total vcore-milliseconds taken by all reduce tasks=4308
                Total megabyte-milliseconds taken by all map tasks=3756032
                Total megabyte-milliseconds taken by all reduce tasks=4411392
        Map-Reduce Framework
                Map input records=1
                Map output records=5
                Map output bytes=50
                Map output materialized bytes=55
                Input split bytes=107
                Combine input records=5
                Combine output records=4
                Reduce input groups=4
                Reduce shuffle bytes=55
                Reduce input records=4
                Reduce output records=4
                Spilled Records=8
                Shuffled Maps =1
                Failed Shuffles=0
                Merged Map outputs=1
                GC time elapsed (ms)=159
                CPU time spent (ms)=1110
                Physical memory (bytes) snapshot=300744704
                Virtual memory (bytes) snapshot=4129456128
                Total committed heap usage (bytes)=141033472
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=30
        File Output Format Counters
                Bytes Written=33
[root@master hadoop-2.7.3]# hdfs dfs -cat /user/hadoop/output/part-r-00000
data    2
mining  1
on      1
warehouse       1
 */