/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SQL2MR {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, FloatWritable> {
    
    private Text outputKey = new Text();
    private FloatWritable outputValue = new FloatWritable();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	     
      String[] toks = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"); 

      float repl_cost = Float.parseFloat(toks[toks.length - 3]);
      String rating = toks[toks.length - 2];
      int len = Integer.parseInt(toks[toks.length - 4]);

      if (len >= 60) {
        outputKey.set(rating);
        outputValue.set(repl_cost);

        context.write(outputKey, outputValue);
      }

       
	
    }
  }
  
  public static class FloatAvgReducer 
       extends Reducer<Text,FloatWritable,Text,FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int cnt = 0;
      int numOf_rep_cost = 0;
      float avg_cost = 0;
        
      for (FloatWritable val: values){
       float repl_cost = val.get();

       float sum = sum += repl_cost;
       numOf_rep_cost++;

       avg_cost = sum / numOf_rep_cost;
       cnt++;
      }

      if(cnt >= 160){
       result.set(avg_cost);
       context.write(key, result);
      }
     
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: sql2mr <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(SQL2MR.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(FloatAvgReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

//film_id,	title,	description,	release_year,	language_id,	original_language_id,	rental_duration,	rental_rate,	length,	replacement_cost,	rating,	special_features
//1, "ACADEMY DINOSAUR", "Teacher in The Canadian Rockies", "2006", 1, 0, 6, 0.99, 86, 20.99,"PG","Deleted Scenes Behind the Scenes"

