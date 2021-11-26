import org.json.JSONObject;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
* extract the "business.json" and "review.json" from the aggregated dataset with multiple types of record
* 
*/



public class CleanMapper extends Mapper<LongWritable, Text, Text, Text> {
    
 
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
       try{ 
            JSONObject jo = new JSONObject(value.toString());
            // business.json has length 14, review.json has length 9
            if (jo.length()==9 || jo.length()==14){
                // use business_id as key for later joining operation
                context.write(new Text(jo.getString("business_id")), value);
            }  
        }
	catch(Exception e){
        // some JSON records are corrupted, discard them
           return;
        }
        
    }
}
