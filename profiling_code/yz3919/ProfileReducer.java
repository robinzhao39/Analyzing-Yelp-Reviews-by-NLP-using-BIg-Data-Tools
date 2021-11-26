import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class ProfileReducer extends Reducer<Text, IntWritable, Text, IntWritable> {


/*
* reduce to count the number of records for eaching rating interval
*/
 
   @Override
   public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

      // add up the total number of review with a certain rating
      int sum = 0;
      for (IntWritable i: values){
         sum += i.get();
      }
      context.write(key, new IntWritable(sum));
   }
}