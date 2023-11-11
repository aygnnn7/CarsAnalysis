package BigDataCourse.CarsProject;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CarsMPGMapper extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, DoubleWritable>{
    
	String filter;
	public void configure(JobConf job) {
		filter = job.get("filter", App.NO_FILTER);
	}
	
	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {
		String[] columns = value.toString().split(";");
		
		String carBrand;
		double mpg;
		
		if(filter.equalsIgnoreCase(App.NO_FILTER) || columns[0].toLowerCase().contains(filter.toLowerCase())) {
			try {
				carBrand = columns[0];
				mpg = Double.parseDouble(columns[2]);
				output.collect(new Text(carBrand), new DoubleWritable(mpg));
			}catch(NumberFormatException ex) {
				System.err.println("Error in -> ." + value.toString() 
				+ "\n" + ex.getMessage());
			}
		}
		
	}


}

