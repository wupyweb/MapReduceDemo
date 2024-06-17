package MapReduce.InvertedIndex;

import java.io.IOException;
import java.util.Hashtable;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.Iterator;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 题目网址：https://www.educoder.net/tasks/62fi9uoajeyt?subject_id=l4c3oyf6
 * 知识点：倒排索引，combiner, hashmap
 */
public class InvertedIndex {
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException

        {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();

            String word;
            IntWritable frequence=new IntWritable();
            int one=1;
            Hashtable<String,Integer>	hashmap=new Hashtable();//key关键字设置为String
            StringTokenizer itr = new StringTokenizer(value.toString());

//****请用hashmap定义的方法统计每一行中相同单词的个数，key为行值是每一行对应的偏移****//
/*********begin*********/
            while (itr.hasMoreTokens()) {
                String str = itr.nextToken();
                if (hashmap.containsKey(str)) {
                    hashmap.put(str, hashmap.get(str)+one);
                } else {
                    hashmap.put(str, one);
                }
            }




/*********end**********/


            for(Iterator<String> it=hashmap.keySet().iterator();it.hasNext();){
                word=it.next();
                frequence=new IntWritable(hashmap.get(word));
                Text fileName_frequence = new Text(fileName+"@"+frequence.toString());//以<K2,“单词 文件名@出现频次”> 的格式输出
                context.write(new Text(word),fileName_frequence);
            }

        }
    }

    public static class InvertedIndexCombiner extends Reducer<Text,Text,Text,Text>{
        protected void reduce(Text key,Iterable<Text> values,Context context)
                throws IOException ,InterruptedException{
//****请合并mapper函数的输出，并提取“文件@1”中‘@’后面的词频，以<K2,list(“单词 文件名@出现频次”)>的格式输出****//
/*********begin*********/

            Hashtable<String,Integer>	hashmap=new Hashtable();    // 文件名为key,单词频次为value
            for (Text value : values) {
                String s = value.toString();

                String filename = s.split("@")[0];
                Integer f = Integer.parseInt(s.split("@")[1]);

                if (hashmap.containsKey(filename)) {
                    hashmap.put(filename, hashmap.get(filename) + f);
                } else {
                    hashmap.put(filename, f);
                }
            }

            for(Iterator<String> it=hashmap.keySet().iterator();it.hasNext();){
                String filename = it.next();
                IntWritable frequence=new IntWritable(hashmap.get(filename));
                Text fileName_frequence = new Text(filename+"@"+frequence.toString());
                context.write(key,fileName_frequence);
            }


/*********end**********/

        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>
    {	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
    {	Iterator<Text> it = values.iterator();
        StringBuilder all = new StringBuilder();
        if(it.hasNext())  all.append(it.next().toString());
        for(;it.hasNext();) {
            all.append(";");
            all.append(it.next().toString());
        }
//****请输出最终键值对list(K3，“单词", “文件1@频次; 文件2@频次;...")****//
/*********begin*********/
        context.write(key, new Text(String.valueOf(all)));




/*********end**********/
    }
    }

    public static void main(String[] args)
    {

        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

            Job job = new Job(conf, "invertedindex");
            job.setJarByClass(InvertedIndex.class);
            job.setMapperClass(InvertedIndexMapper.class);
            //****请为job设置Combiner类****//
/*********begin*********/
            job.setCombinerClass(InvertedIndexCombiner.class);
/*********end**********/
            job.setReducerClass(InvertedIndexReducer.class);

            job.setOutputKeyClass(Text.class);
            //****请设置输出value的类型****//
/*********begin*********/
            job.setOutputValueClass(Text.class);
/*********end**********/

            //job.setNumReduceTasks(0);     // 只运行map阶段，不运行reduce阶段
            FileInputFormat.addInputPath(job, new Path("D:\\hadoop\\input2"));
            FileOutputFormat.setOutputPath(job, new Path("D:\\hadoop\\output2"));

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

