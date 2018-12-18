import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.NotionalTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.*;

import java.io.IOException;
import java.util.List;

public class WordsTotalDoc {
    public static class WordsPerDocMapper extends Mapper<LongWritable, Text,Text,Text> {
        private final static IntWritable one = new IntWritable(1);
        private Text sepword =new Text();

        //<word#docId, wordcount>-> <docId,(word=count)>
        @Override
        public void map(LongWritable offset,Text line,Context context ) throws IOException,InterruptedException{
            Text key=new Text();
            Text value = new Text();

            String []temp1=line.toString().split("\\s+");
            //System.out.println(Arrays.toString(temp1));
            String wordCount=temp1[1];

            String []temp2=temp1[0].toString().split("#");
            System.out.println(Arrays.toString(temp2));
            String docId=temp2[1];
            String word=temp2[0];
            System.out.println(word+','+docId+','+wordCount);

            key.set(docId);
            value.set(word+'='+wordCount);
            context.write(key,value);
        }
    }

    public static class WordsPerDocReducer extends Reducer<Text,Text,Text,Text> {
        Text valueOut= new Text();
        Text keyOut=new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)throws IOException,InterruptedException{

            String docId=key.toString();
            int wordsPerDoc=0;
            Map<String, String> tempCounter= new HashMap<String,String> ();
            for (Text wordWordCount :values){
                String []temp1=wordWordCount.toString().split("=");
                //System.out.println(Arrays.toString(temp1));
                String wordCount=temp1[1];
                int wordCountInt=Integer.parseInt(wordCount);
                wordsPerDoc+=wordCountInt;
                tempCounter.put(temp1[0],temp1[1]);
            }
            //System.out.println(values.toString());

            for (String val: tempCounter.keySet()) {
                keyOut.set(val);
                valueOut.set(tempCounter.get(val)+','+docId+","+String.valueOf(wordsPerDoc));
                //System.out.println(keyOut.toString()+valueOut.toString());
                context.write(keyOut,valueOut);
            }
        }
    }
}
