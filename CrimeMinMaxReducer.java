package mr_demo.CrimeMinMaxDate;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;



public class CrimeMinMaxReducer extends Reducer<Text, CrimeMinMaxTuple, Text, CrimeMinMaxTuple> {

private CrimeMinMaxTuple result = new CrimeMinMaxTuple();


@Override 
public void reduce( Text key, Iterable<CrimeMinMaxTuple> values, Context context) 
		throws IOException,  InterruptedException
{

result.setMin(null); 
result.setMax(null);



for (CrimeMinMaxTuple val : values)
{
if (result.getMin() == null ||

val.getMin().compareTo(result.getMin()) < 0) 

{

result.setMin(val.getMin());

}

if (result.getMax() == null ||

val.getMax().compareTo(result.getMax()) > 0)
{

result.setMax(val.getMax());

}



}



context.write(key, result);
}
}
