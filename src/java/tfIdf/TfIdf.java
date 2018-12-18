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

public class TfIdf {
    public static class TfIdfMapper extends Mapper<LongWritable, Text,Text,Text> {

        @Override
        public void map(LongWritable offset,Text line,Context context ) throws IOException,InterruptedException{
            Text key=new Text();
            Text value = new Text();

            String []temp1=line.toString().split("\\s+");
            key=new Text(temp1[0]);
            value= new Text(temp1[1]);
            context.write(key,value);
        }
    }

    public static class TfIdfReducer extends Reducer<Text,Text,Text,Text> {
        Text valueOut= new Text();
        Text keyOut=new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)throws IOException,InterruptedException{
            String word=key.toString();
            int docsPerWord=0;
            Map<String, String> tempCounter= new HashMap<String,String> ();
            for (Text val :values){
                //String docId;
                docsPerWord+=1;
                String [] temp1=val.toString().split(",");
                String docId=temp1[1];
                String wordCount=temp1[0];
                String wordsPerDoc=temp1[2];

                tempCounter.put(docId,wordCount+','+wordsPerDoc);
            }
            //System.out.println(values.toString());

            for (String val: tempCounter.keySet()) {
                String []temp2=tempCounter.get(val).split(",");
                System.out.println(word+Arrays.toString(temp2));
                String docId=val;
                double wordCount=Double.valueOf(temp2[0]);
                double wordsPerDoc=Double.valueOf(temp2[1]);
                double tf=wordCount/wordsPerDoc;
                double totalDocs=Double.valueOf(context.getJobName());// set into the context
                double idf=totalDocs/(docsPerWord+1);
                double tfIdf=10000*tf*Math.log10(idf);//enlarge in
                keyOut.set(word+','+docId);
                //valueOut.set(String.valueOf(tf)+","+String.valueOf(idf)+","+String.valueOf(tfIdf));
                //System.out.println(keyOut.toString()+valueOut.toString());
                valueOut.set(String.valueOf(tfIdf));
                context.write(keyOut,valueOut);
            }
        }
    }

}
