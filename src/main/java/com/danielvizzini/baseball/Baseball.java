package com.danielvizzini.baseball;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Baseball extends Configured implements Tool {

	public static final int MIN_NUM_ATBATS = 100;
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, TotalsArrayWritable> {
		
		//legacy code from tutorial
		static enum Counters { INPUT_LINES }

		private long numRecords = 0;
		private String inputFile;

		@Override
		public void configure(JobConf job) {
			inputFile = job.get("map.input.file");
		}

		//create queues so dates do not need to be recalculated
		ArrayDeque<String> batterQueue = new ArrayDeque<String>();
		ArrayDeque<Integer> pitchesQueue = new ArrayDeque<Integer>();
		ArrayDeque<Integer> rbisQueue = new ArrayDeque<Integer>();
		ArrayDeque<Integer> basesQueue = new ArrayDeque<Integer>();
		
		//keep track of inning totals
		int pitchesInInning = 0;
		int rbisInInning = 0;
		int basesInInning = 0;

		public void map(LongWritable key, Text value, OutputCollector<Text, TotalsArrayWritable> output, Reporter reporter) throws IOException {
			
			//ready input
			String file = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(file, "|");
			
			//instantiate to non-existent for comparison
			int[] halfInning = {0,0};
			
			//begin iteration through batters
			while (true) {
								
				//iterate
				String line = (tokenizer.hasMoreTokens()) ? tokenizer.nextToken() : null;
				String[] csv = {}; 	
				
				//even if line is null, last half-inning will still need to be collected
				if (line != null) {
					//break up records
					csv = line.split(",");

					//ignore badly formatted
					if(csv.length != 7) continue;					

					//ignore non-play records
					if(!csv[0].equals("play")) continue;
					
				}
				
				//mark end of half-inning (short-circuiting prevents null pointer)
				if (line == null || Integer.parseInt(csv[1]) != halfInning[0] || Integer.parseInt(csv[2]) != halfInning[1]) {
					
					//populate last half-inning
					while (!batterQueue.isEmpty()) {
						
						//instantiate and set key
						String batterString = batterQueue.pop();
						Text batter = new Text();
						batter.set(batterString);					
						
						//Keep track of batters to avoid double counting in case order bats around
						HashSet<String> playersInHalfInning = new HashSet<String>();

						/**
						 * Value to be passed to reducer
						 * index 0: number of pitches
						 * index 1: pitches in inning
						 * index 2: num batter's rbis
						 * index 3: rbis in inning
						 * index 4: num batter's bases
						 * index 5: bases in inning
						 */
						IntWritable[] mappedValues = new IntWritable[6];
						
						//set values
						mappedValues[0] = new IntWritable(pitchesQueue.pop());
						mappedValues[1] = new IntWritable(playersInHalfInning.contains(batterString) ? 0 : pitchesInInning);
						mappedValues[2] = new IntWritable(rbisQueue.pop());
						mappedValues[3] = new IntWritable(playersInHalfInning.contains(batterString) ? 0 : rbisInInning);
						mappedValues[4] = new IntWritable(basesQueue.pop());
						mappedValues[5] = new IntWritable(playersInHalfInning.contains(batterString) ? 0 : basesInInning);
						
						playersInHalfInning.add(batterString);
						
						//store for Hadoop
						output.collect(batter, new TotalsArrayWritable(mappedValues));
						reporter.incrCounter(Counters.INPUT_LINES, 1L);
						
					}
					
					//having collected last half-inning, break
					if (line == null) break;
					
					//update half inning
					halfInning[0] = Integer.parseInt(csv[1]);
					halfInning[1] = Integer.parseInt(csv[2]);
					
					//reset values
					batterQueue = new ArrayDeque<String>();
					pitchesQueue = new ArrayDeque<Integer>();
					rbisQueue = new ArrayDeque<Integer>();
					basesQueue = new ArrayDeque<Integer>();
					pitchesInInning = 0;
					rbisInInning = 0;
					basesInInning = 0;
					
				}
						
				//mark batter
				batterQueue.add(csv[3]);
				
				//calculate statistics
				int pitches = BaseballUtil.countPitches(csv[5]);
				int rbis = BaseballUtil.countRbis(csv[6]);
				int bases = BaseballUtil.countBases(csv[6]);
				
				//update totals
				pitchesInInning += pitches;
				rbisInInning += rbis;
				basesInInning += bases;
				
				//update queues
				pitchesQueue.add(pitches);
				rbisQueue.add(rbis);
				basesQueue.add(bases);
				
			}

			//legacy code from tutorial
			reporter.setStatus("Finished processing " + ++numRecords  + " records " + "from the input file: " + inputFile);
		}
	}

	public static class Combine extends MapReduceBase implements Reducer<Text, TotalsArrayWritable, Text, TotalsArrayWritable> {

		public void reduce(Text key, Iterator<TotalsArrayWritable> values, OutputCollector<Text, TotalsArrayWritable> output, Reporter reporter) throws IOException {
			
			TotalsArrayWritable totals = new TotalsArrayWritable();		
			totals.combine(values);
						
			output.collect(key, totals);
			
		}				
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, TotalsArrayWritable, Text, FloatArrayWritable> {

		public void reduce(Text key, Iterator<TotalsArrayWritable> values, OutputCollector<Text, FloatArrayWritable> output, Reporter reporter) throws IOException {
			
			TotalsArrayWritable totals = new TotalsArrayWritable();		
			totals.combine(values);

			//only include batter's with all-positive denominators
			if (totals.denominatorIsZero()) return;				
				
			//only include batters with MIN_NUM_ATBATS
			if (totals.getNumAtBats() < MIN_NUM_ATBATS) return;
			
			//record aggregate statistics
			output.collect(key, totals.getRatios());
			
		}				
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), Baseball.class);
		conf.setJobName("baseball");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(TotalsArrayWritable.class);
		
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Combine.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		return 0;

	}
	
	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new Baseball(), args);
	    System.exit(res);
	    
	}
	
}