package BigDataCourse.CarsProject;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CarsListReducer extends MapReduceBase

implements Reducer<Text, Text, Text, Text>{

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
       
	        while(values.hasNext()) {
	            String[] parts = values.next().toString().split("&");
	           
	            double hp = Double.parseDouble(parts[0]);
	            double mpg = Double.parseDouble(parts[1]);
	           
	            String outputHP = String.valueOf(hp);
	            String outputMPG = String.valueOf(mpg);
	            
	            if(hp <= 0){
	            	outputHP = "NO_DATA";
	            }
	            if(mpg <= 0) {
	                outputMPG = "NO_DATA";
	            }
	            
	            App.OutputCounter++;
	            output.collect(key, new Text(outputHP + "  " + outputMPG));
	        }
        
        	
        }
	}	

