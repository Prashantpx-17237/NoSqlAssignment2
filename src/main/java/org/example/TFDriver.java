package org.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class TFDriver {

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Must pass InputPath and OutputPath and Path to Top100 files in local directory.");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("filepath", args[2]);
        Job job = Job.getInstance(conf, "Stripes");
        job.setJarByClass(TFDriver.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        job.setMapperClass(TFMapper.class);
        //job.setCombinerClass(Reduce.class); // enable this to use 'local aggregation'
        job.setReducerClass(TFReducer.class);

        conf.set("dfFile", args[2]);


        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
