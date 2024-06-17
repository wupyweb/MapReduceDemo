package MapReduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * KEYIN, reduce阶段输入的key类型Text
 * VALUEIN, reduce阶段输入的value的类型IntWritable
 * KEYOUT, reduce阶段输出的key类型Text
 * VALUEOUT, reduce阶段输出的value类型IntWritable
 */
public class WordCountReducer  extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable OutV = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        OutV.set(sum);

        context.write(key, OutV);
    }
}
