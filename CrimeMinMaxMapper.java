package mr_demo.CrimeMinMaxDate;


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//import mr_demo.CrimeFirstLastDate.MRDPUtils;
//import mr_demo.CrimeFirstLastDate.MinMaxCountTuple;

public class CrimeMinMaxMapper extends Mapper<Object, Text, Text, CrimeMinMaxTuple> {
	// Our output key and value Writables
	private Text crimeType = new Text();
	private CrimeMinMaxTuple outTuple = new CrimeMinMaxTuple();

	// This object will format the creation date string into a Date object
	private final static SimpleDateFormat frmt = new SimpleDateFormat(
			"yyyy-MM-dd");

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		 String line = value.toString();
	     String Type = line.split(",")[0];
	     String strDate = line.split(",")[1]+"-"+line.split(",")[2]+"-"+line.split(",")[3];
		 
	     


		// get will return null if the key is not there
		if (strDate == null || Type == null) {
			// skip this record
			return;
		}

		try {
			// Parse the string into a Date object
			Date creationDate = frmt.parse(strDate);

			// Set the minimum and maximum date values to the creationDate
			outTuple.setMin(creationDate);
			outTuple.setMax(creationDate);
			// Set our type as the output key
			crimeType.set(Type);

			// Write out the user ID with min max dates and count
			context.write(crimeType, outTuple);
		} catch (ParseException e) {
			// An error occurred parsing the creation Date string
			// skip this record
		}
	}

}
