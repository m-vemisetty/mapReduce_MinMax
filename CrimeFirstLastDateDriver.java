package mr_demo.CrimeMinMaxDate;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CrimeFirstLastDateDriver {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: Frequency <input path> <output path>");
      System.exit(-1);
    }
    
    Job job = Job.getInstance();
    job.setJarByClass(CrimeFirstLastDateDriver.class);
    job.setJobName("CrimeFirstLastDateDriver");

    // set the input and output path
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

  
    job.setMapperClass(CrimeMinMaxMapper.class);
    job.setReducerClass(CrimeMinMaxReducer.class);

    // specify the type of the output
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrimeMinMaxTuple.class);

    // run the job
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}