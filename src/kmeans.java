import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class kmeans {
	public static final ArrayList<String[]> Centroids2 = new ArrayList<>();
	public static final ArrayList<String[]> Centroids1 = new ArrayList<>();
    public static class Map extends Mapper
        <Object, Text, Text, Text> {
            private Text word = new Text();
            private Text word1 = new Text();
            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            	
                String line = value.toString();
                StringTokenizer tokenizer = new StringTokenizer(line);
                String[] rt = new String[2];
                while (tokenizer.hasMoreTokens()) {
                	rt = tokenizer.nextToken().split(",");
                }
                double[] distance = new double[Centroids1.size()];
                for(int i=0;i<Centroids1.size();i++) {
                	distance[i] = computedistance(Centroids1.get(i),rt);
                }
                
                int index = findmindistance(distance);
                word =  new Text(index+","+Centroids1.get(index)[0]+","+Centroids1.get(index)[1]);
                word1 = new Text(rt[0]+","+rt[1]);
                
                context.write(word,word1);
        
            }
            public int findmindistance(double[] s) {
            	int min_ind =0;
            	double min_value = s[0];
            	for(int i= 0 ;i<s.length;i++) {
            		if(s[i]<min_value) {
            			min_ind = i;
            			min_value = s[i];
            		}
            	}
            	return min_ind;
            }
            
            public double computedistance(String[] a, String[] b) {
            	return Math.sqrt(Math.pow(Double.parseDouble(a[0])-Double.parseDouble(b[0]),(double)2) + Math.pow(Double.parseDouble(a[1])-Double.parseDouble(b[1]),(double)2));
            }
        }
    
    
    public static class Reduce extends Reducer
        <Text, Text, Text, Text> {
            public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            	String temp = key.toString(); 
            	String[] rt = temp.split(",");            	
            	int sumx = 0;
            	int sumy = 0;
            	int n = 0;
            	for(Text val:values ) {
            		String temp1 = val.toString();
            		String[] temp2 = temp1.split(",");
            		sumx = sumx+Integer.parseInt(temp2[0]);
            		sumy = sumy+Integer.parseInt(temp2[1]);
            		n++;
            	}
            	int centroid_x = (int)sumx/n;
            	int centroid_y = (int)sumy/n;
            	String[] ncn = new String[2];
            	ncn[0] = Integer.toString(centroid_x);
            	ncn[1] = Integer.toString(centroid_y);
            	
            	
            	Centroids2.add(Integer.parseInt(rt[0]), ncn);
            	System.out.println(ncn[0]+","+ncn[1]);
            }
        }
    
    	public static boolean containsornot(int[] index,int temp) {
    		for(int i = 0 ;i<index.length;i++) {
    			if(index[i]==temp) {
    				return true;
    			}
    		}
    		return false;
    	}
        public static void main(String[] args) throws Exception {
        	
        	FileReader fileReader = new FileReader(args[0]);
        	BufferedReader bufferedReader = new BufferedReader(fileReader);
        	ArrayList<String[]> data_points= new ArrayList<>(); 
        	String line;
        	while((line = bufferedReader.readLine()) != null) {
        		data_points.add(line.split(","));
        	}
        	bufferedReader.close();
        	
        	int num_of_clusters = Integer.parseInt(args[2]);
        	int[] index = new int[num_of_clusters];
        	for(int i=0;i<num_of_clusters;i++) {
        		index[i] = data_points.size();
        	}
        	for(int i=0;i<num_of_clusters;i++) {
        		Centroids1.add(data_points.get(i));
        		Centroids2.add(data_points.get(i));
        		System.out.println(data_points.get(i)[0]+data_points.get(i)[1]);
        	}
        	
        	        	
        	Configuration conf = new Configuration();
            conf.set("mapreduce.job.queuename", "apg_p7");
            Job job = new Job(conf);
            job.setJarByClass(kmeans.class);
            job.setJobName("myWordCount");
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapperClass(kmeans.Map.class);
            job.setCombinerClass(kmeans.Reduce.class);
            job.setReducerClass(kmeans.Reduce.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);
        }
}
