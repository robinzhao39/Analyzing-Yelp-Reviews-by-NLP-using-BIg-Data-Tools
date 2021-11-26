import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.math.BigDecimal;
import org.json.JSONObject;

public class CleanReducer extends Reducer<Text, Text, NullWritable, Text> {


/*
* join business.json records with review.json records using the business_id field
* extract review_id, business_id, review_rating, business_rating, review_text 
* 
*/
 
   @Override
   public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      String reviewId = "";
      String businessId = "";
      String review_text = "";
      Float review_rating = 0f;
      Float business_rating = 0f;
      
      for (Text t:values){
         
            JSONObject jo = new JSONObject(t.toString());
            // handling review.json
            if (jo.length()==9){
               reviewId = jo.getString("review_id");
               businessId = jo.getString("business_id");
               review_text = jo.getString("text");
               review_rating = BigDecimal.valueOf(jo.getDouble("stars")).floatValue();
            }
            else {
               //handling business.json
               business_rating = BigDecimal.valueOf(jo.getDouble("stars")).floatValue();
            }
         }

      //map everything to a new JSON object
      Map<String,String> hash = new HashMap<String, String>();
      hash.put("review_id",reviewId);
      hash.put("business_id",businessId);
      hash.put("review_rating",review_rating.toString());
      hash.put("business_rating",business_rating.toString());
      hash.put("review_text",review_text);
      JSONObject newObj = new JSONObject(hash);

      // write out JSON
      Text result = new Text(newObj.toString());
      context.write(NullWritable.get(), result);
   }
}
