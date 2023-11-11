package BigDataCourse.CarsProject;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.net.URI;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
@SuppressWarnings("serial")
public class App extends JFrame
{
	public static final String NO_FILTER = "all";
	public static final char FILTER_SPLITTER = '#';
	public static int OutputCounter = 0;
	
	private static final String EMPTY_STRING = "";
	private static Boolean SelectedTypeIsMPG = false;
	private JPanel warningPanel;
    private JLabel lblWarning;
    
    
	public void Init() {
		JPanel headerPanel = new JPanel();
		final JLabel lblHeader = new JLabel("Данни за автомобили");
		lblHeader.setFont(new Font("Arial", Font.ITALIC, 22));
	    headerPanel.add(lblHeader);
	    
		JPanel formPanel = new JPanel();
        formPanel.setLayout(new GridLayout(5, 2, 10, 10));

        // label result type
        final JLabel lblResult = new JLabel("Тип резултат:");
        formPanel.add(lblResult);

        // drop down result type
        final String[] resultTypes = {"Списък коли", "Среден разход по марка" };
        final JComboBox<String> cbResultTypes = new JComboBox<String>(resultTypes);
        formPanel.add(cbResultTypes);
        
        // label search car by brand
        JLabel lblCarByBrand = new JLabel("Кола по марка:");
        formPanel.add(lblCarByBrand);
        
        // text field search car by brand
        final JTextField tfCarByBrand = new JTextField();
        formPanel.add(tfCarByBrand);

        // label minimum horsepower
        final JLabel lblMinHP = new JLabel("По конски сили от:");
        formPanel.add(lblMinHP);
        
        // text field minimum horsepower
        final JTextField tfMinHP = new JTextField();
        formPanel.add(tfMinHP);
        
        // label maximum horsepower
        final JLabel lblMaxHP = new JLabel("По конски сили до:");
        formPanel.add(lblMaxHP);
        
        // text field maximum horsepower
        final JTextField tfMaxHP = new JTextField();
        formPanel.add(tfMaxHP);
        
        // label minimum MPG
        final JLabel lblMinMPG = new JLabel("Среден разход от:");
        formPanel.add(lblMinMPG);
        
        // text field minimum MPG
        final JTextField tfMinMPG = new JTextField();
        formPanel.add(tfMinMPG);
       
        //label warning
        warningPanel = new JPanel();
        lblWarning = new JLabel("");
        warningPanel.add(lblWarning);
        
        // button search
        JPanel buttonPanel = new JPanel();
        JButton searchB = new JButton("Търсене");
        buttonPanel.add(searchB);
        												
        //frame settings
        JPanel mainPanel = new JPanel();
        headerPanel.setPreferredSize(new Dimension(450,50));
        formPanel.setPreferredSize(new Dimension(450,200));
        warningPanel.setPreferredSize(new Dimension(450,70));
        buttonPanel.setPreferredSize(new Dimension(450,70));
        
        mainPanel.add(headerPanel);
        mainPanel.add(formPanel);
        mainPanel.add(warningPanel);
        mainPanel.add(buttonPanel);
        add(mainPanel);
        setBounds(50, 50, 500, 425);
        setVisible(true);
        
        cbResultTypes.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                String selectedResultType = (String) cbResultTypes.getSelectedItem();
               
                //set up UI for MPG by brand
                if (resultTypes[1].equals(selectedResultType)) {
                	SelectedTypeIsMPG = true;
                	tfMinHP.setText(EMPTY_STRING);
    				tfMaxHP.setText(EMPTY_STRING);
    				tfMinMPG.setText(EMPTY_STRING);
    				
    				tfMinHP.setVisible(false);
    				tfMaxHP.setVisible(false);
    				tfMinMPG.setVisible(false);
    				
    				lblMinHP.setVisible(false);
    				lblMaxHP.setVisible(false);
    				lblMinMPG.setVisible(false);
                } 
                //set up UI for List cars
                else if (resultTypes[0].equals(selectedResultType)) {
                	SelectedTypeIsMPG = false;
                	tfMinHP.setVisible(true);
    				tfMaxHP.setVisible(true);
    				tfMinMPG.setVisible(true);
    				
    				lblMinHP.setVisible(true);
    				lblMaxHP.setVisible(true);
    				lblMinMPG.setVisible(true);
                }
            }
        });
        
        searchB.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				lblWarning.setText("");
				OutputCounter = 0;
				String filters = "";
				String brand = tfCarByBrand.getText(),
						minHP = tfMinHP.getText(),
						maxHP = tfMaxHP.getText(),
						minMPG = tfMinMPG.getText();
				
				if(!isNumericOrEmpty(minHP) ||
						!isNumericOrEmpty(maxHP) ||
						!isNumericOrEmpty(minMPG))
				{
					lblWarning.setForeground(Color.RED);
					lblWarning.setText("Последните три полета трябва да са числа.");
				}
				else {
					if(!brand.isEmpty()) filters += FILTER_SPLITTER + brand;
					else filters += FILTER_SPLITTER + NO_FILTER;
					
					if(!minHP.isEmpty()) filters += FILTER_SPLITTER + minHP;
					else filters += FILTER_SPLITTER + NO_FILTER;
					
					if(!maxHP.isEmpty()) filters += FILTER_SPLITTER +maxHP;
					else filters += FILTER_SPLITTER + NO_FILTER;
					
					if(!minMPG.isEmpty()) filters += FILTER_SPLITTER + minMPG;
					else filters += FILTER_SPLITTER + NO_FILTER;
					
					startHadoopJob(filters);	
				}
			}
		});
	}
	
	protected static boolean isNumericOrEmpty(String strNum) {
		if(strNum.isEmpty()) return true;
        try {
            @SuppressWarnings("unused")
			double d = Double.parseDouble(strNum);
        } catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }
    
	protected void startHadoopJob(String filters) {
		Configuration conf = new Configuration();
		JobConf job = new JobConf(conf, App.class);
		
		//check for unnecessesary FILTER_SPLITTER at the beginning of the filters
		if(filters.charAt(0) == FILTER_SPLITTER) filters = filters.substring(1, filters.length());
		
		//if "среден разход по марка" selected
		if(SelectedTypeIsMPG) {
			//filters brand#minHP#maxHP#minMPG
			String[] filtersArr = filters.split(String.valueOf(FILTER_SPLITTER));
			job.set("filter", filtersArr[0]);
			job.setMapperClass(CarsMPGMapper.class);
			job.setReducerClass(CarsMPGReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
		}
		//if "списък коли" selected
		else {
			job.set("filters", filters);
			job.setMapperClass(CarsListMapper.class);
			job.setReducerClass(CarsListReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
		}
		
		Path inputPath = new Path("hdfs://127.0.0.1:9000/input/cars.csv");
        Path outputPath = new Path("hdfs://127.0.0.1:9000/carsResults");
        
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        try {
			FileSystem fs = FileSystem.get(
					URI.create("hdfs://127.0.0.1:9000"), conf);
			
			if(fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}	
			RunningJob task = JobClient.runJob(job);
			if(task.isSuccessful()) {
				lblWarning.setForeground(Color.BLUE);
				if(OutputCounter <= 0) {
					lblWarning.setText("Няма подходящ автомобил за вашите филтри.");
				}else {
					if(OutputCounter == 1)
						lblWarning.setText(OutputCounter + " автомобил, подходящ за вашите филтри, e записан в txt файла.");
					else
						lblWarning.setText(OutputCounter + " автомобила, подходящи за вашите филтри, са записани в txt файла.");
				}
			}
				
		} catch (IOException e) {
			
			lblWarning.setForeground(Color.RED);
			lblWarning.setText(e.getMessage());
		}
	}
	
    public static void main( String[] args )
    {
    	App form = new App();
        form.Init();
    }
    
    
}
