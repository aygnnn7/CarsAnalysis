package BigDataCourse.CarsProject;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CarsMPGReducer extends MapReduceBase

implements Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output,
			Reporter reporter) throws IOException {
		
		double sum = 0;
		double counter = 0;
		while(values.hasNext()) {
			sum += values.next().get();
			counter++;
		}
		double totalMPG = sum/counter;
		
		App.OutputCounter++;
		output.collect(key, new DoubleWritable(Math.round(totalMPG*100.0)/100.0));
		
		
	}

}
