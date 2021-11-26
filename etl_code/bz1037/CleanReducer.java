import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class CleanReducer
 extends Reducer<Text, Text, IntWritable, Text > {

 @Override
 public void reduce(Text key, Iterable<Text> values, Context context)
 throws IOException, InterruptedException {

	  int index=0;
      for (Text val : values) {
    	  context.write(new IntWritable(index), val);
    	  index++;
      }

}
}

