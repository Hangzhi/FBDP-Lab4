import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;



public class KNNDriver{
    private static int k;
    private static Path testPath;

    /**
     * args= <trainset path> <output path> <testset path> <neighbour num>
     */
    public static void main(String[] args) throws IOException,InterruptedException {
        BasicConfigurator.configure();
        Job kNNJob = new Job();
		kNNJob.setJobName("kNNJob");
		kNNJob.setJarByClass(KNNDriver.class);
        testPath=new Path(args[2]);
		DistributedCache.addCacheFile(URI.create(args[2]), kNNJob.getConfiguration());
		kNNJob.getConfiguration().setInt("k", Integer.parseInt(args[3]));

        Path tempPath= new Path(args[1]+"KnnPredict"+Long.toString(System.nanoTime()));
		try {
            kNNJob.setMapperClass(KNNMapper.class);
            kNNJob.setMapOutputKeyClass(Text.class);
            kNNJob.setMapOutputValueClass(Text.class);

            kNNJob.setReducerClass(KNNReducer.class);
            kNNJob.setOutputKeyClass(Text.class);
            kNNJob.setOutputValueClass(Text.class);

            kNNJob.setInputFormatClass(TextInputFormat.class);
            kNNJob.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(kNNJob, new Path(args[0]));
            FileOutputFormat.setOutputPath(kNNJob, tempPath);

            System.exit(kNNJob.waitForCompletion(true) ? 0 : 1);
        }catch (Exception e){
            e.printStackTrace();
        }
		System.out.println("finished!");
    }

    /**
     * In:<offset,trainLine>
     * Out:<testId,(label,distance)>
     */
    public static class KNNMapper extends Mapper<LongWritable, Text, Text,Text>{
        private static ArrayList<Instance> testSet=new ArrayList<Instance>();

        /** load the testFile and split it to an ArrayList */
        @Override
        protected void setup(Context context)throws IOException,InterruptedException{
            k=context.getConfiguration().getInt("k",1);
            ArrayList<Instance>trainSet= new ArrayList<Instance>();

            /*
            Path[] testFiles= DistributedCache.getLocalCacheFiles(context.getConfiguration());


            for(int i=0; i<testFiles.length; i++){
            */
            BufferedReader br= null;
            String line;
            Path tempPath= testPath;
                int count=0;
                String stockName=tempPath.toString().split("\\.")[0];
                br= new BufferedReader(new FileReader(tempPath.toString()));
                while((line=br.readLine())!=null){
                    count+=1;
                    String []tempSplit=line.split(" ");
                    String lineId=tempSplit[0];
                    Instance testInstance=new Instance(line);
                    testInstance.setId(lineId);
                    testSet.add(testInstance);
                }

        }

        /**
         * compute the distance to every testLine
         * emit<testId,(label,distance)>
         */

         @Override
         public void map(LongWritable offset, Text trainLine, Context context)throws IOException, InterruptedException{
            /**
            //ArrayList<Double>distance= new ArrayList<double>(testSet.length);
             //ArrayList<String>trainLabel= new ArrayList<String>(testSet.length);
             for(int i=0; i<k; i++){
                 distance.add(Double.MAX_Value);
                 trainLabel.add("cl0");
             }*/

             Text key= new Text();
             Text value= new Text();

             //ListWritable<Text> labels= new ListWritable<Text>;
             Instance trainInstance= new Instance(trainLine.toString());

             //System.out.println(String.valueOf(testSet.size()));
             for(int i=0; i<testSet.size();i++){
                 System.out.println(testSet.get(i).getAtrributeValue().length);
                 System.out.println(trainInstance.getAtrributeValue().length);
                 System.out.println("-------");
                 try {
                     if(testSet.get(i).getAtrributeValue().length!= trainInstance.getAtrributeValue().length){
                         System.out.println(testSet.get(i).getAtrributeValue());
                         System.out.println(trainInstance.getAtrributeValue());
                         System.out.println(testSet.get(i).getId());
                     }
                     double dis = Distance.EuclideanDistance(testSet.get(i).getAtrributeValue(), trainInstance.getAtrributeValue());
                     double label = trainInstance.getLabel();
                     key.set(testSet.get(i).getId());
                     value.set(label + "," + String.valueOf(dis));
                     context.write(key, value);
                 } catch(Exception e){
                     e.printStackTrace();
                 }
             }

         }
    }

    /**
     * In:<testId,(label,distance)...>
     * Out<testId,label>
     */
    public static class KNNReducer extends Reducer <Text, Text, Text, Text>{

        @Override
        public void reduce(Text testId,Iterable<Text>LDs,Context context)throws IOException,InterruptedException{
            Text key = new Text();
            Text value= new Text();

            ArrayList<Double>distances=new ArrayList<Double>(k);
            ArrayList<DoubleWritable>trainLabels=new ArrayList<DoubleWritable>(k);
            DoubleWritable minusOne= new DoubleWritable();
            minusOne.set(-1);
            for(int i=0;i<k;i++){
                distances.add(Double.MAX_VALUE);
                trainLabels.add(minusOne);
            }
            for(Text LD:LDs){
                try{
                String [] tmpSplits=LD.toString().split(",");
                DoubleWritable tmpLabel= new DoubleWritable();
                tmpLabel.set(Double.valueOf(tmpSplits[0]));
                DoubleWritable trainLabel=tmpLabel;
                Double dis=Double.valueOf(tmpSplits[1]);
                int index= indexOfMax(distances);
                if(dis<distances.get(index)){
                    distances.remove(index);
                    trainLabels.remove(index);
                    distances.add(dis);
                    trainLabels.add(trainLabel);
                }
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
            ListWritable<DoubleWritable> labels = new ListWritable<DoubleWritable>();
            labels.setList(trainLabels);

            //decide the predicted label
            DoubleWritable predictedLabel;
            //DoubleWritable one=new DoubleWritable(1.0);
            //DoubleWritable two=new DoubleWritable(2.0);
            //DoubleWritable three=new DoubleWritable(3.0);
            try{
                predictedLabel=valueOfMostFrequent(labels);
                String typeFlag=(String.valueOf(predictedLabel));
                System.out.println("|"+typeFlag+"|");
                if (typeFlag.equals("1.0")){
                    value.set("negative");
                }
                else if(typeFlag.equals("2.0")) {
                    value.set("neutral");
                }
                else if(typeFlag.equals("3.0")) {
                    value.set("positive");
                }
            }catch(Exception e){
                //TODO Auto-generated catch block
                e.printStackTrace();
            }
            context.write(testId,value);
        }


        public int indexOfMax(ArrayList<Double> array){
			int index = -1;
			Double min = Double.MIN_VALUE;
			for (int i = 0;i < array.size();i++){
				if(array.get(i) > min){
					min = array.get(i);
					index = i;
				}
			}
			return index;
        }

        public DoubleWritable valueOfMostFrequent(ListWritable<DoubleWritable> list) throws Exception{
			if(list.isEmpty())
				throw new Exception("list is empty!");
			else{
				HashMap<DoubleWritable,Integer> tmp = new HashMap<DoubleWritable,Integer>();
				for(int i = 0 ;i < list.size();i++){
					if(tmp.containsKey(list.get(i))){
						Integer frequence = tmp.get(list.get(i)) + 1;
						tmp.remove(list.get(i));
						tmp.put(list.get(i), frequence);
					}else{
						tmp.put(list.get(i), new Integer(1));
					}
				}
				//find the value with the maximum frequence.
				DoubleWritable value = new DoubleWritable();
				Integer frequence = new Integer(Integer.MIN_VALUE);
				Iterator<Entry<DoubleWritable, Integer>> iter = tmp.entrySet().iterator();
				while (iter.hasNext()) {
				    Map.Entry<DoubleWritable,Integer> entry = (Map.Entry<DoubleWritable,Integer>) iter.next();
				    if(entry.getValue() > frequence){
				    	frequence = entry.getValue();
				    	value = entry.getKey();
				    }
				}
				return value;
			}
		}
    }

}