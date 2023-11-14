package uni.fmi.Cars;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CarsMapper extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, DoubleWritable>{

	String resultType;
	String selectedBrand;
	Double selectedHorsepowerFrom;
	Double selectedHorsepowerTo;
	Double minimalMpg;

	
	@Override
	public void configure(JobConf job) {
		resultType = job.get("resultType", "");
		selectedBrand = job.get("brand", "");
		selectedHorsepowerFrom = Double.parseDouble(job.get("horsepowerFrom", "0"));
		selectedHorsepowerTo = Double.parseDouble(job.get("horsepowerTo", String.valueOf(Integer.MAX_VALUE)));
		minimalMpg = Double.parseDouble(job.get("mpg", "0"));
	}

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {
	
		try {
			String[] columns = value.toString().split(";");
			
			 // Extract relevant information
	        String brand = columns[0];
	        double horsepower = Double.parseDouble(columns[5]);
	        double mpg = Double.parseDouble(columns[2]);
	       
	        if (resultType.contentEquals("0")) {
	        	boolean horsepowerMatch = horsepower >= selectedHorsepowerFrom && horsepower <= selectedHorsepowerTo;
	            boolean mpgMatch = mpg >= minimalMpg;
	            boolean brandMatch = StringUtils.containsIgnoreCase(brand, selectedBrand);
	            
	            if (brandMatch && mpgMatch && horsepowerMatch) {
	            	Text outputKey = new Text(brand + " " 
							+ horsepower);
					
					output.collect(outputKey, new DoubleWritable(mpg));    
	            }
	        }
	        else if (resultType.contentEquals("1")) {
	            boolean brandMatch = StringUtils.containsIgnoreCase(brand, selectedBrand);
	            
	            if (brandMatch) {
	            	Text outputKey = new Text(brand);
					
					output.collect(outputKey, new DoubleWritable(mpg));
	            }
	        }
		}
		catch (NumberFormatException e) {
			e.printStackTrace();
		}
	}
}
