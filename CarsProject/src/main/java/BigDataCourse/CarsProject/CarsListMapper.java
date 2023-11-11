package BigDataCourse.CarsProject;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class CarsListMapper extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, Text> {

	String filters;
	public void configure(JobConf job) {
		filters = job.get("filters", App.NO_FILTER);
	}
    public void map(LongWritable key, Text value, 
    		OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
    	
        String[] columns = value.toString().split(";");
        String brand;
        double hp;
        double mpg;
        
        try {
        	brand = columns[0];
      		hp = Double.parseDouble(columns[5]);
      		mpg = Double.parseDouble(columns[2]);
      		
      		 //filters brand#minHP#maxHP#minMPG
	  		 String[] filtersArr = filters.split(String.valueOf(App.FILTER_SPLITTER));
	  		 
	  		 
	  		 //filtering
	         if((filtersArr[0].equalsIgnoreCase(App.NO_FILTER) || brand.toLowerCase().contains(filtersArr[0])) 
	         		&& (filtersArr[1].equalsIgnoreCase(App.NO_FILTER) || hp >= parseDouble(filtersArr[1], true)) 
	         		&& (filtersArr[2].equalsIgnoreCase(App.NO_FILTER) || hp <= parseDouble(filtersArr[2], false))
	         		&& (filtersArr[3].equalsIgnoreCase(App.NO_FILTER) || mpg >= parseDouble(filtersArr[3], true))) {
	         	output.collect(new Text(brand), new Text(hp + "&" + mpg));
	         }
		}catch(NumberFormatException ex) {
			System.err.println("Error in -> ." + value.toString() 
			+ "\n" + ex.getMessage());
		}
    }
    
    private double parseDouble(String num, boolean isMin) {
    	double result;
    	try {
    		result = Double.parseDouble(num);
			
		}catch(NumberFormatException ex) {
			System.err.println("Error in -> ." + num.toString() 
			+ "\n" + ex.getMessage());
			if(isMin) return -1;
			else return Double.MAX_VALUE;
		}
    	return result;
    }
    
}
