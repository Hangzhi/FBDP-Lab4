import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.NotionalTokenizer;
import org.apache.log4j.BasicConfigurator;


public class WordCountPerDoc{
    private static int fileProcessed=0;
    public static final String GBK = "GBK";

    /**
     *
     * @param args=[inputPath,]
     * @throws Exception
     */

    public static void main(String [] args)throws Exception{
        //BasicConfigurator.configure();
        Configuration conf=new Configuration();
        Path tempPath= new Path("title-interm"+Long.toString(System.nanoTime()));// make tmp directory
        //Path tempPath= new Path("tempPath");// make tmp directory
        //wordCountDocJob
        Job WordCountPerDocJob=new Job(conf,"WordCountPerDocJob"); //initiate a new job with conf and stringname
        WordCountPerDocJob.setJarByClass(WordCountPerDoc.class);// set the jar
        try{
            WordCountPerDocJob.setMapperClass((WordCountPerDocMapper.class));
            WordCountPerDocJob.setReducerClass(WordCountPerDocReducer.class);
            WordCountPerDocJob.setMapOutputKeyClass(Text.class);//the same type of map and reduce ouput class
            WordCountPerDocJob.setMapOutputValueClass(IntWritable.class);
            WordCountPerDocJob.setOutputKeyClass(Text.class);
            WordCountPerDocJob.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(WordCountPerDocJob,new Path(args[0]));
            FileOutputFormat.setOutputPath(WordCountPerDocJob, tempPath);
            //adverseIndexJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            System.out.println(fileProcessed);

           //System.exit(WordCountPerDocJob.waitForCompletion(true) ? 0 : 1);
        }finally{
            //FileSystem.get(conf).deleteOnExit(tempPath);
        }
        Path tempPath2= new Path("wordsPerDoc"+Long.toString(System.nanoTime()));
        //wordsPerDocJob
        Job wordsPerDocJob= new Job(conf, "wordsPerDocJob");
        if(WordCountPerDocJob.waitForCompletion(true)){
            System.out.println("wordsPerDoc begin!");
            wordsPerDocJob.setJarByClass(WordCountPerDoc.class);
            wordsPerDocJob.setMapperClass(WordsTotalDoc.WordsPerDocMapper.class);
            wordsPerDocJob.setReducerClass(WordsTotalDoc.WordsPerDocReducer.class);
            wordsPerDocJob.setOutputValueClass(Text.class);
            wordsPerDocJob.setOutputKeyClass(Text.class);
            FileInputFormat.addInputPath(wordsPerDocJob,tempPath);
            FileOutputFormat.setOutputPath(wordsPerDocJob,tempPath2);
            //System.exit(wordsPerDocJob.waitForCompletion(true) ? 0 : 1);
        }

        Job tfIdfJob=new Job(conf,"tfIdfJob");

        //Path tempPath3=new Path("tfIdf"+Long.toString(System.nanoTime()));
        Path tempPath3=new Path("data/testTfidf");
        if(wordsPerDocJob.waitForCompletion(true)){
            System.out.println("tfIdf begin!");
            tfIdfJob.setJarByClass(WordCountPerDoc.class);
            tfIdfJob.setMapperClass(TfIdf.TfIdfMapper.class);
            tfIdfJob.setReducerClass(TfIdf.TfIdfReducer.class);
            tfIdfJob.setOutputValueClass(Text.class);
            tfIdfJob.setOutputKeyClass(Text.class);
            FileInputFormat.addInputPath(tfIdfJob, tempPath2);
            FileOutputFormat.setOutputPath(tfIdfJob, tempPath3);
            tfIdfJob.setJobName(String.valueOf(470));
            //negative:470  neutral: 511 positive:516
            System.exit(tfIdfJob.waitForCompletion(true) ? 0 : 1);
        }
        System.out.println("Bingo!");

    }
    public static class WordCountPerDocReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable finalCount= new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException,InterruptedException{
            //System.out.println(0);
            int count=0;
            for (IntWritable sepValue: values){
                count+=sepValue.get();
            }
            finalCount.set(count);
            //System.out.println(count);
            context.write(key,finalCount);
        }
    }
    public static Text transformTextToUTF8(Text text, String encoding) throws  UnsupportedEncodingException{
        String value ;
        value = new String (text.getBytes(), 0, text.getLength(), encoding);
        return new Text(value);
    }

    /*
    //override the compare() method to achieve a decreasing queue
    public class IntWritab
    leDecreasingComparator extends IntWritable.Comparator {
        @SuppressWarnings("rawtypes")
        public int compare( WritableComparable a,WritableComparable b){
            return -super.compare(a, b);
        }
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }*/

    public static class WordCountPerDocMapper extends Mapper<LongWritable, Text,Text,IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text sepword =new Text();

        @Override
        public void map(LongWritable offset,Text line,Context context ) throws IOException,InterruptedException{
            //remove all the num and character
            //match several invisible spaces
            line=transformTextToUTF8(line,GBK);
            String lineSplits= line.toString().replaceAll("[^a-zA-Z0-9\u4e00-\u9fa5]", "").replaceAll("[a-zA-Z0-9]","");//.split("\\s*");
            /*
            for (String split:lineSplits){
                System.out.println(split);
            }
            */
            System.out.println(lineSplits);
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            System.out.println(filename);
            //String [] tmp=filename.split("\\.");
            //System.out.println(Arrays.toString(tmp));
            String docId=filename.split("\\.")[0];

            fileProcessed=fileProcessed+1;
            List<Term> titleSplits=NotionalTokenizer.segment(lineSplits);

            for (Term value: titleSplits){
                String key=value.word+'#'+docId;
                sepword.set(key);
                context.write(sepword,one);
            }
        }
    }
}