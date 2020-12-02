import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.regex.Pattern;

public class Mapreduce extends Configured implements Tool {


    private static final Logger LOG = Logger.getLogger(Mapreduce.class);
//    public String filename;


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Mapreduce(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "wordcount");
        job.setJarByClass(this.getClass());

//        filename = job.getJobFile().toString();

// Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Map.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        return job.waitForCompletion(true) ? 0 : 1;
    }


    //    FUNKCJA MAPUJACA
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        //        private Text word = new Text();
//        private long numRecords = 0;
        private IntWritable passengerCount = new IntWritable();
        private Text puLocationID = new Text();
        boolean paymentTypeIs2;
        private int i;


        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");


        //
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {


            context.getInputSplit();
            Path filePath = ((FileSplit) context.getInputSplit()).getPath();
            String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

            String month = "asdasd";
            int counter = 0;
            for (String word : fileName.split("[-.]")){
                if(counter==1){
                    month = word;
                }
                counter++;
            }
//            klucz - puLocation,miesiąc (który trzeba pobrać z nazwy pliku)
//            wartość - passengerCount
//            warunek wysłania do context.write - paymentTypeIs2
            try {
                if (key.get() == 0)
                    return;
                else {
                    String line = value.toString();
                    i = 0;

                    for (String word : line.split(",")) {

                        if (i == 3) {
                            passengerCount.set(Integer.parseInt(word));
                        } else if (i == 7) {
                            puLocationID.set(month + "," + word+",");
                        } else if (i == 9 && word.equals("2")) {
                            context.write(puLocationID, passengerCount);
                        }
                        i++;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    //    Combiner
    public static class Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();
        Float average;
        Float count;
        int sum;


        //        okej oblicza srednia
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            average = 0f;
            count = 0f;
            sum = 0;
            Text sumText = new Text(key);
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(sumText, result);
        }


    }

    //    FUNKCJA REDUKUJĄCA
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();
        Float average;
        Float count;
        int sum;


        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            average = 0f;
            count = 0f;
            sum = 0;
            Text sumText = new Text(key);
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(sumText, result);
        }


    }
}