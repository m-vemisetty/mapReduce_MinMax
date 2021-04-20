package mr_demo.CrimeMinMaxDate;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Writable;
public class CrimeMinMaxTuple implements Writable {
    private Date min = new Date();   
    private Date max = new Date();    
    private final static SimpleDateFormat frmt = 
		new SimpleDateFormat( "yyyy-MM-dd");
    public Date getMin() { return min; } 
    public void setMin(Date min) { this.min = min; } 
    public Date getMax() { return max; } 
    public void setMax(Date max) { this.max = max; } 
    
    public void readFields(DataInput in) throws IOException {
	min = new Date(in.readLong()); 
	max = new Date(in.readLong()); 
	}
    public void write(DataOutput out) throws IOException{
	out.writeLong(min.getTime());
	out.writeLong(max.getTime());
	}

    public String toString() {return frmt.format(min) + "\t" + frmt.format(max) ; 
    }
} 