import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
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


public class Word2Vec{
    private static int k;
    private static ArrayList<String> featureList= new ArrayList<String>();
    private static String cl_now;
    /**
     *
     * @param args=[inputPath, featuresPath, cl(num)]
     * @throws Exception
     */

    public static void main(String [] args)throws Exception{
        //BasicConfigurator.configure();

        cl_now=args[2];
        Configuration conf=new Configuration();
        String filename=args[0].substring(args[0].lastIndexOf("/")+1);
        //k=Integer.parseInt(args[2]);
        Path tempPath= new Path(filename+"word2vec"+Long.toString(System.nanoTime()));// make tmp directory
        //Path tempPath= new Path("tempPath");// make tmp directory
        //wordCountDocJob

        FileInputStream inputStream = new FileInputStream(args[1]);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        String str = null;
        while((str=bufferedReader.readLine())!=null){
            featureList.add(str);
        }
        inputStream.close();
        bufferedReader.close();

        Job Word2VecJob=new Job(conf,"Word2VecJob"); //initiate a new job with conf and stringname
        Word2VecJob.setJarByClass(Word2Vec.class);// set the jar
        try{
            Word2VecJob.setMapperClass((Word2VecMapper.class));
            Word2VecJob.setReducerClass(Word2VecReducer.class);
            Word2VecJob.setMapOutputKeyClass(Text.class);//the same type of map and reduce ouput class
            Word2VecJob.setMapOutputValueClass(Text.class);
            Word2VecJob.setOutputKeyClass(Text.class);
            Word2VecJob.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(Word2VecJob,new Path(args[0]));
            FileOutputFormat.setOutputPath(Word2VecJob, tempPath);
            //adverseIndexJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            //System.out.println(fileProcessed);

            // System.exit(Word2VecJob.waitForCompletion(true) ? 0 : 1);
        }finally {
            if (Word2VecJob.waitForCompletion(true)) {
                System.out.println("Bingo!");
            }
        }

    }

    /**
     * In <offset,(<word,docId>,tfIdf>
     * Out <docId, <word, tfIdf>>
     */
    public static class Word2VecMapper extends Mapper<LongWritable, Text, Text ,Text>{

        @Override
        public void map(LongWritable offset, Text line, Context context)throws IOException,InterruptedException{
            Text key= new Text();
            Text value= new Text();

            String [] split1= line.toString().split("\t");
            String [] split2= split1[0].split(",");

            String word=split2[0];
            Double tfIdf=Double.valueOf(split1[1]);
            String docId=split2[1];

            //System.out.println(line.toString());

            key.set(docId);
            value.set(word+","+String.valueOf(tfIdf));

            System.out.println(key.toString());
            System.out.println(value.toString());

            context.write(key,value);
        }


    }

    /**
     * In <docId, <word, tfIdf>...>
     * Out <docId  (tfIdfVec, cln)>
     */

    public static class Word2VecReducer extends Reducer<Text, Text,Text, Text>{

        @Override
        public void reduce(Text docId, Iterable<Text> values,Context context)throws IOException,InterruptedException{
            Text key = new Text();
            Text value= new Text();

            Map<String,Double>tfIdfs=new HashMap<String,Double>();
            String vec="";

            for(Text val: values){
                String []splitTmp=val.toString().split(",");
                tfIdfs.put(splitTmp[0],Double.valueOf(splitTmp[1]));
            }

            for(String currWord:featureList){
                if(tfIdfs.containsKey(currWord)){
                    vec=vec+" "+tfIdfs.get(currWord);
                }
                else{
                    vec=vec+" "+"0";
                }
            }

            vec+=" "+cl_now;

            key.set(docId.toString());
            value.set(vec);

            System.out.println(key.toString());
            System.out.println(value.toString());

            context.write(key, value);

        }
    }

}