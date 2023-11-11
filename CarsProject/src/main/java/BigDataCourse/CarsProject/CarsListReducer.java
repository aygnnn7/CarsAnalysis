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
		double totalHP = 0.0;
		double totalMpg = 0.0;
		int countHP = 0;
		int countMPG = 0;
        
       
        while(values.hasNext()) {
            String[] parts = values.next().toString().split("&");
           
            double hp = Double.parseDouble(parts[0]);
            double mpg = Double.parseDouble(parts[1]);
            
            if(hp > 0 ) {
	        	 totalHP += hp;
	             countHP++;
            }
            if(mpg > 0) {
                totalMpg += mpg;
                countMPG++;
            }
        }
        
        double averageHP = 0;
        double averageMPG = 0;
        if (countHP > 0)  averageHP = totalHP / countHP;
        if (countMPG > 0) averageMPG = totalMpg / countMPG;
        
        	App.OutputCounter++;
            output.collect(key, new Text(String.format("%.1f", averageHP) + " " + String.format("%.1f", averageMPG)));
        }
	}

