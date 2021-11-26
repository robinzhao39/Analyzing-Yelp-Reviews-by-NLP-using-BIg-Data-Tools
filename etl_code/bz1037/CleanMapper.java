import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper
 extends Mapper<LongWritable, Text, Text, Text> {

public void map(LongWritable key, Text value, Context context)
 throws IOException, InterruptedException {
	 String data=value.toString();
	 data=data.replace("{", "");
	 data=data.replace("}", "");
	 data=data.replace("\"","");
	 String jparts[]=data.split(",");
	 String output="";
	 
	 for (String i: jparts) {
		 String pair[]=i.split(":");
		if(pair[0].equals("business_id") || pair[0].equals("name")||pair[0].equals("stars"))
		 output=output+pair[1]+", ";		 
	 }
		
	 output=output.substring(0, output.length()-2);
	 context.write(new Text("text"),new Text(output));
	 }
}
