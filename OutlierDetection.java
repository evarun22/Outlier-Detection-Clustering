package Project2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import java.util.*;
import java.lang.Math.*;

public class OutlierDetection {

    //public static int radius,threshold;
    
    static List<String> createZones(int maxX, int maxY, int width, int height) {
        List<String> allZones = new ArrayList<>();
        int id = 0;
        for (int bl_y = 0; bl_y < maxX; bl_y++) {
        	int tr_y = bl_y + height - 1;
        	for (int bl_x = 0; bl_x < maxY; bl_x++) {
        		int tr_x = bl_x + width - 1;
        		id+=1;
        		allZones.add(Integer.toString(id) + "," + Integer.toString(bl_x) + "," + Integer.toString(bl_y) + "," + Integer.toString(tr_x) + "," + Integer.toString(tr_y));
        		bl_x = tr_x;
        	}
        	bl_y = tr_y;
        }
        return allZones;
    }
    
    public static List<String> allZones = createZones(10000,10000,1000,1000);
    
	public static class PointMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] point = value.toString().split(",");
			int pX = Integer.parseInt(point[0]);
			int pY = Integer.parseInt(point[1]);
			Configuration conf = context.getConfiguration();
			int radius = Integer.parseInt(conf.get("r"));
			int threshold = Integer.parseInt(conf.get("t"));
			//System.out.println(allZones.size());
			for  (String zone: allZones) {
				String[] zone_ = zone.split(",");
				int id = Integer.parseInt(zone_[0]);
				int bl_x = Integer.parseInt(zone_[1]);
				int bl_xr = bl_x - radius;
				int bl_y = Integer.parseInt(zone_[2]);
				int bl_yr = bl_y - radius;
				int tr_x = Integer.parseInt(zone_[3]);
				int tr_xr = tr_x + radius;
				int tr_y = Integer.parseInt(zone_[4]);
				int tr_yr = tr_y + radius;
				//System.out.println(id+" "+bl_x+" "+bl_xr+" "+bl_y+" "+bl_yr+" "+tr_x+" "+tr_xr+" "+tr_y+" "+tr_yr+"\n");
				if ((bl_x <= pX) && (tr_x >= pX) && (bl_y <= pY) && (tr_y >= pY)) {
					String type = "point";
					//System.out.println(pX+" "+pY+" "+type+" "+id);
					context.write(new IntWritable(id), new Text(type + "," + Integer.toString(pX) + "," + Integer.toString(pY)));
				}
				else if ((bl_xr <= pX) && (tr_xr >= pX) && (bl_yr <= pY) && (tr_yr >= pY)) {
					String type = "border-point";
					//System.out.println(pX+" "+pY+" "+type+" "+id);
					context.write(new IntWritable(id), new Text(type + "," + Integer.toString(pX) + "," + Integer.toString(pY)));
				}
			}
		}
	}
	
	public static class CustomReducer extends Reducer<IntWritable, Text, Text, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<String> points = new ArrayList<>();
			//System.out.println(key);
			List<String> borderPoints = new ArrayList<>();
			List<String> allPoints = new ArrayList<>();
    		Configuration conf = context.getConfiguration();
			int radius = Integer.parseInt(conf.get("r"));
			int threshold = Integer.parseInt(conf.get("t"));
			for (Text val:values) {
				//System.out.println(val);
				String[] types = val.toString().split(",");
				String type = types[0];
				String pX = types[1];
				String pY = types[2];
				String xy = pX + "," + pY;
				//System.out.println(type);
				if (type.equals("point"))
				    //System.out.println(xy);
					points.add(xy); 
				else 
					borderPoints.add(xy);
			}
			allPoints.addAll(points);
			allPoints.addAll(borderPoints);
			//Collections.sort(allPoints);
    		//System.out.println(points.size());
    		int cr = radius;
    		double radiusSquared = Math.pow(cr,2);
    		for (String point : points) {
    			//System.out.println("\n"+xy);
    			String[] point_ = point.split(",");
    			int x = Integer.parseInt(point_[0]);
    			int y = Integer.parseInt(point_[1]);
    			List<String> inPoints = new ArrayList<>();
    			for (String allPoint : allPoints) {
    				String[] allPoint_ = allPoint.split(",");
    				int _x = Integer.parseInt(allPoint_[0]);
    				int _y = Integer.parseInt(allPoint_[1]);
    				double distanceSquared = Math.pow((x - _x),2) + Math.pow((y - _y),2);
    				if (distanceSquared <= radiusSquared) 
    					inPoints.add(allPoint);
    			}
    			//System.out.println("\n"+inPoints.size());
    			if (inPoints.size() - 1 < threshold) 
    				context.write(new Text(point), new Text(Integer.toString(inPoints.size())));
    		}
    	}
    }
    
    public static void main(String[] args) throws Exception {
    
    	if (args.length < 4) {
    		System.out.println("ERROR-----------Enter 4 arguments----------input file path, output file path, radius and threshold in the same order");
    		System.exit(1);
    	}
    	Configuration conf = new Configuration();
    	FileSystem.get(conf).delete(new Path(args[1]), true);
    	conf.set("r",args[2]);
    	conf.set("t",args[3]);
    	Job job = new Job(conf);
    	job.getInstance(conf, "OutlierDetection");
        job.setJarByClass(OutlierDetection.class);
        job.setMapperClass(PointMapper.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setReducerClass(CustomReducer.class);
        job.setNumReduceTasks(100);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean jobStatus = job.waitForCompletion(true);
        System.exit(jobStatus ? 0 : 1);
    }
}
    

