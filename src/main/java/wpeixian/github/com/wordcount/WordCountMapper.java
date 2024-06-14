package wpeixian.github.com.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN, map阶段输入的key类型LongWritable
 * VALUEIN, map阶段输入的value的类型Text
 * KEYOUT, map阶段输出的key类型Text
 * VALUEOUT, map阶段输出的value类型IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text OutK = new Text();
    private IntWritable OutV = new IntWritable(1);


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1. 获取一行
        String line = value.toString();

        // 2. 切割
        String[] words = line.split(" ");

        // 3. 输出
        for (String word : words) {

            OutK.set(word);
            context.write(OutK, OutV);
        }

    }
}
