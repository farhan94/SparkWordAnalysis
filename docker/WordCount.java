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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

  /**
   * map() gets a key, value, and context (which we'll ignore for the moment).
   * key - seems to be "bytes from the beginning of the file"
   * value - the current line; we are being fed one line at a time from the
   *         input file
   *
   * here's what the key and value look like if i print them out with the first
   * println statement below:
   *
   * [map] key: (0), value: ([Weekly Compilation of Presidential Documents])
   * [map] key: (47), value: (From the 2002 Presidential Documents Online via GPO Access [frwais.access.gpo.gov])
   * [map] key: (130), value: ([DOCID:pd04fe02_txt-11]                         )
   * [map] key: (179), value: ()
   * [map] key: (180), value: ([Page 133-139])
   *
   * in the tokenizer loop, each token is a "word" from the current line, so the first token from
   * the first line is "Weekly", then "Compilation", and so on. as a result, the output from the loop
   * over the first line looks like this:
   *
   * [map] key: (0), value: ([Weekly Compilation of Presidential Documents])
   * [map, in loop] token: ([Weekly)
   * [map, in loop] token: (Compilation)
   * [map, in loop] token: (of)
   * [map, in loop] token: (Presidential)
   * [map, in loop] token: (Documents])
   *
   */
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      // System.err.println(String.format("[map] key: (%s), value: (%s)", key, value));
      // break each sentence into words, using the punctuation characters shown
      StringTokenizer tokenizer = new StringTokenizer(value.toString(), " \t\n\r\f,.:;?![]'");
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable occurrencesOfWord = new IntWritable();

    public void reduce(Text key,
    		               Iterable<IntWritable> values,
                       Context context)
    throws IOException, InterruptedException
    {
      // debug output
      // printKeyAndValues(key, values);
      // the actual reducer work
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      occurrencesOfWord.set(sum);
      // this writes the word and the count, like this: ("Africa", 2)
      context.write(key, occurrencesOfWord);
      // my debug output
      // System.err.println(String.format("[reduce] word: (%s), count: (%d)", key, occurrencesOfWord.get()));
    }

    // a little method to print debug output
  //   private void printKeyAndValues(Text key, Iterable<IntWritable> values)
  //   {
  //     StringBuilder sb = new StringBuilder();
  //     for (IntWritable val : values)
  //     {
  //       sb.append(val.get() + ", ");
  //     }
  //     System.err.println(String.format("[reduce] key: (%s), value: (%s)", key, sb.toString()));
  //   }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
