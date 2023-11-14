package uni.fmi.Cars;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CarsReducer extends MapReduceBase
	implements Reducer<Text,DoubleWritable,Text,DoubleWritable>{
	
	String resultType;

	@Override
	public void configure(JobConf job) {
		resultType = job.get("resultType", "");
	}

	@Override
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output,
			Reporter reporter) throws IOException {
	
		
		if (resultType.contentEquals("0")) {
			output.collect(key, new DoubleWritable(values.next().get()));
		}
		else if (resultType.contentEquals("1")) {
			double sum = 0;
	        int count = 0;

	        while(values.hasNext()) {
				sum += values.next().get();
				count++;
			}

	        double average = sum / count;

	        output.collect(key, new DoubleWritable(average));
		}
	}
}
