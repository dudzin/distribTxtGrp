package pl.pw.elka.distmatrix;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class DistMatrixDriver {

  public static void main(String[] args) throws Exception {

	  Path inputPath = new Path(args[0]);
      Path outputDir = new Path(args[1]);

      // Create configuration
      Configuration conf = new Configuration(true);
      initConf(conf);
      // Create job
      Job job = new Job(conf, "WordCount");
      job.setJarByClass(DistMatrixDriver.class);
      
      // Setup MapReduce
      job.setMapperClass(DistMatrixMapper.class);
      //job.setReducerClass(WordCountReducer.class);
      job.setNumReduceTasks(0);

      // Specify key / value
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      // Input
      FileInputFormat.addInputPath(job, inputPath);
      job.setInputFormatClass(DocTextInputFormat.class);
      
      // Output
      FileOutputFormat.setOutputPath(job, outputDir);
      job.setOutputFormatClass(TextOutputFormat.class);

      // Delete output if exists
      FileSystem hdfs = FileSystem.get(conf);
      if (hdfs.exists(outputDir))
          hdfs.delete(outputDir, true);

      // Execute job
      int code = job.waitForCompletion(true) ? 0 : 1;
      System.exit(code);
  }
  
  private static void initConf(Configuration conf){
	        
      conf.set("distMatrixMapper.stemmer","Porter");
      conf.set("distMatrixMapper.forbWords","conf/forbWords.txt");
      conf.set("distMatrixMapper.regexForm","conf/regexForm.txt");

  }
  
}

