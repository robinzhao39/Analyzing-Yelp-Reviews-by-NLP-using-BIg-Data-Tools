import org.json.JSONObject;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.math.BigDecimal;

/*
* find the distribution of review rating and business rating with a 0.1 interval
* 
*/



public class ProfileMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    
 
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            JSONObject jo = new JSONObject(value.toString());
            if (jo.length()==9 || jo.length()==14){
                float rating = BigDecimal.valueOf(jo.getDouble("stars")).floatValue();
                if (jo.length()==9)
                    context.write(new Text(String.format("review_rating=%f:",rating)), new IntWritable(1));
                else
                    context.write(new Text(String.format("business_rating=%f:",rating)), new IntWritable(1));
            }  
        } catch (Exception e){
            return;
        }
        
    }
}