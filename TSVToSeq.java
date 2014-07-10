package org.apache.mahout.text;

/*
 * Copyright (c) 2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

/**
 * Converts a tab separated value (.tsv) file on the local file system
 * to a Hadoop Sequence file and stores that file to the specified
 * HDFS directory 
 */
public class TSVToSeq {
	public static void main(String args[]) throws Exception {
		
		  // Check if command inputs meet requirements
		  // Example: $ TSVToSeq.jar org.apache.mahout.text.TSVToSeq InputFileName OutputDir
	      if (args.length != 2) {
	          System.out.println("usage: [input] [output]");
	          System.exit(-1);
	        }
		
		String inputFileName = args[0]; // Name of the file on the local file system to process
		String outputDirName = args[1]; //Name of the folder within HDFS to be used as output
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
				
		Writer writer = new SequenceFile.Writer(fs, conf, new Path(outputDirName + "/chunk-0"),
				Text.class, Text.class);
		
		int count = 0; // Initialize counter
		
		BufferedReader reader = new BufferedReader(new FileReader(inputFileName));
		
		Text key = new Text();
		Text value = new Text();
		
		while(true) {
			// for each question
			String line = reader.readLine();
			if (line == null) {
				break;
			}
			
			// if the key/value separator is something other than a 'tab' character, replace it below
			String[] tokens = line.split("\t"); // split key and values using 'tab' character as seperator

			String id = tokens[0]; // assign key to string 'id'
			String message = tokens[1]; // assign value to string 'message'

			key.set("/" + id);
			value.set(message);
			writer.append(key, value); // write to sequence file with given key and value
			count++; // increment counter
		}
		reader.close(); // Close reader
		writer.close(); // Close writer
		System.out.println("Wrote " + count + " entries."); // Output number of lines processed
	}
}
