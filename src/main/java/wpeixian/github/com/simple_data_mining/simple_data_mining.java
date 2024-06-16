package wpeixian.github.com.simple_data_mining;

import java.io.IOException;
import java.util.*;

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

/**
 * 下面给出一个child-parent的表格，要求挖掘其中的父子辈关系，给出祖孙辈关系的表格。
 *
 * 输入文件内容如下：
 * child          parent
 * Steven        Lucy
 * Steven        Jack
 * Jone         Lucy
 * Jone         Jack
 * Lucy         Mary
 * Lucy         Frank
 * Jack         Alice
 * Jack         Jesse
 * David       Alice
 * David       Jesse
 * Philip       David
 * Philip       Alma
 * Mark       David
 * Mark       Alma
 */
public class simple_data_mining {
    public static int time = 0;

    /**
     * @param args
     * 输入一个child-parent的表格
     * 输出一个体现grandchild-grandparent关系的表格
     */
    //Map将输入文件按照空格分割成child和parent，然后正序输出一次作为右表，反序输出一次作为左表，需要注意的是在输出的value中必须加上左右表区别标志
    public static class Map extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
            /********** Begin **********/
            String line = value.toString();
            String[] split = line.split("\\s+");

            context.write(new Text(split[0]), new Text("R:"+split[1]));
            context.write(new Text(split[1]), new Text("L:"+split[0]));


            /********** End **********/
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException,InterruptedException{
            /********** Begin **********/

            //输出表头
            if (time==0) {
                context.write(new Text("grand_child"), new Text("grand_parent"));
                time++;
            }

            List<String> childList = new ArrayList<>();
            List<String> parentList = new ArrayList<>();
            //获取value-list中value的child
            for (Text value : values) {
                String s = value.toString();

                if (s.startsWith("L:")) {
                    childList.add(s.substring(2));
                }
                if (s.startsWith("R:")) {
                    parentList.add(s.substring(2));
                }
            }

            Integer sum = 0;


            //获取value-list中value的parent




            //左表，取出child放入grand_child




            //右表，取出parent放入grand_parent




            //输出结果
            boolean b = !childList.isEmpty() && !parentList.isEmpty();
            if(b){
                for (String c : childList) {
                    for (String p : parentList) {
                        context.write(new Text(c), new Text(p));
                    }
                }
            }

            /********** End **********/

        }
    }
    public static void main(String[] args) throws Exception{
        // TODO Auto-generated method stub
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Single table join");
        job.setJarByClass(simple_data_mining.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String inputPath = "/user/reduce/input";   //设置输入路径
        String outputPath = "/user/reduce/output";   //设置输出路径
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}

/**
 * grand_child	grand_parent
 * Mark	Jesse
 * Mark	Alice
 * Philip	Jesse
 * Philip	Alice
 * Jone	Alice
 * Jone	Jesse
 * Steven	Alice
 * Steven	Jesse
 * Steven	Mary
 * Steven	Frank
 * Jone	Mary
 * Jone	Frank
 */
