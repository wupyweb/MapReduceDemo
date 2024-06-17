package MapReduce.test;

import java.io.IOException;

import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Merge {

    /**
     * @param args
     * 对A,B两个文件进行合并，并剔除其中重复的内容，得到一个新的输出文件C
     */
    //在这重载map函数，直接将输入中的value复制到输出数据的key上 注意在map方法中要抛出异常：throws IOException,InterruptedException
    public static class Map extends Mapper<LongWritable, Text, Text, Text>
    {
        /********** Begin **********/
        private Text outk = new Text();
        private Text outv = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] parts = line.split("\t");

            outk.set(parts[0]);
            outv.set(parts[1]);
            context.write(outk, outv);
        }


        /********** End **********/

    }



    //在这重载reduce函数，直接将输入中的key复制到输出数据的key上  注意在reduce方法上要抛出异常：throws IOException,InterruptedException
    public static class  Reduce  extends Reducer<Text, Text, Text, Text>
    {
        /********** Begin **********/
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<String> list = new ArrayList<>();
            for (Text value : values) {
                if (!list.contains(value.toString())) {
                    list.add(value.toString());
                }
            }
            Collections.sort(list);

            for (String s : list) {
                context.write(key, new Text(s));
            }
        }


        /********** End **********/

    }





    public static void main(String[] args) throws Exception{

        // TODO Auto-generated method stub
        Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://localhost:9000");

        Job job = Job.getInstance(conf,"Merge and duplicate removal");
        job.setJarByClass(Merge.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String inputPath = "/user/tmp/input/";  //在这里设置输入路径
        String outputPath = "/user/tmp/output/";  //在这里设置输出路径

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
