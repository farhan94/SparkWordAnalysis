import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.lang.InterruptedException;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// Do not change the signature of this class
public class TextAnalyzer extends Configured implements Tool {

    public static class Entry implements Comparable<Entry> {
      private String queryWord;
      private Integer appearences;

      public Entry(String queryWord) {
        this.queryWord = queryWord;
        appearences = 1;
      }

      public Entry(String qw, String app) {
        queryWord = qw;
        appearences = Integer.parseInt(app);
      }

      @Override
      public boolean equals(Object o) {
        if(o != null && o instanceof Entry) {
          Entry e = (Entry) o;
          return queryWord.equals(e.getQueryWord());
        }

        return false;
      }

      @Override
      public int hashCode() {
        int h = 3;
        return 7 * h  + this.queryWord.hashCode();
      }

      @Override
      public int compareTo(Entry e) {
        return queryWord.compareToIgnoreCase(e.getQueryWord());
      }

      @Override
      public String toString() {
        return queryWord + " " + appearences;
      }

      public String getQueryWord() { return queryWord; }
      public Integer getAppearences() { return appearences; }
      public void setAppearences(int x) { appearences = x; }
    }

    // Replace "?" with your own output key / value types
    // The four template data types are:
    //     <Input Key Type, Input Value Type, Output Key Type, Output Value Type>
    private static Text word = new Text();
    private static Text tuple = new Text();
    public static class TextMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
            String line = value.toString().toLowerCase().replaceAll("\\W+", " ").trim();
            String[] contextWords = line.split("\\s+");
            String[] queryWords = line.split("\\s+");
            Set<String> seen = new HashSet<>();

            for(int i = 0; i < contextWords.length; i += 1) {
              String contextWord = contextWords[i];
              List<Entry> list = new ArrayList<>();
              if(seen.add(contextWord)) {
                for(int j = 0; j < queryWords.length; j += 1) {
                  String queryWord = queryWords[j];
                  if(i != j) {
                    Entry e = new Entry(queryWord);
                    int chk = list.indexOf(e);
                    if(chk == -1) {
                      list.add(e);
                    }
                    else {
                      Entry entry = list.get(chk);
                      entry.setAppearences(entry.getAppearences() + 1);
                      list.set(chk, entry);
                    }
                  }
                }

                word.set(contextWord);
                Collections.sort(list);

                for(Entry e : list) {
                  tuple.set(e.toString());
                  context.write(word, tuple);
                }
              }
            }
        }
    }


    // Replace "?" with your own key / value types
    // NOTE: combiner's output key / value types have to be the same as those of mapper
    public static class TextCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> queryTuples, Context context)
            throws IOException, InterruptedException
        {

            List<Entry> combinedList = new ArrayList<>();
            for(Text qt : queryTuples) {
              String[] tuple = qt.toString().split("\\s+");
              Entry e = new Entry(tuple[0], tuple[1]);
              int i = combinedList.indexOf(e);
              if(i == -1) {
                combinedList.add(e);
              }
              else {
                Entry entry = combinedList.get(i);
                entry.setAppearences(e.getAppearences() + entry.getAppearences());
                combinedList.set(i, entry);
              }
            }


            Collections.sort(combinedList);
            word.set(key);

            for(Entry e : combinedList) {
              tuple.set(e.toString());
              context.write(word, tuple);
            }
        }
    }

    // Replace "?" with your own input key / value types, i.e., the output
    // key / value types of your mapper function
    private static final Text emptyText = new Text("");
    private static Text queryWordText = new Text();
    private static Text queryWordCount = new Text();
    public static class TextReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> queryTuples, Context context)
            throws IOException, InterruptedException
        {
            context.write(key, emptyText);

            List<Entry> combinedList = new ArrayList<>();
            for(Text qt : queryTuples) {
              String[] tuple = qt.toString().split("\\s+");
              Entry e = new Entry(tuple[0], tuple[1]);
              int i = combinedList.indexOf(e);
              if(i == -1) {
                combinedList.add(e);
              }
              else {
                Entry entry = combinedList.get(i);
                entry.setAppearences(e.getAppearences() + entry.getAppearences());
                combinedList.set(i, entry);
              }
            }

            Collections.sort(combinedList);

            for(Entry e : combinedList) {
              queryWordText.set("<" + e.getQueryWord() + ",");
              queryWordCount.set(e.getAppearences().toString() + ">");
              context.write(queryWordText, queryWordCount);
            }

            context.write(emptyText, emptyText);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        // Create job
        Job job = new Job(conf, "jja2244_jcc4428"); // Replace with your EIDs
        job.setJarByClass(TextAnalyzer.class);

        // Setup MapReduce job
        job.setMapperClass(TextMapper.class);
        //   Uncomment the following line if you want to use Combiner class
        job.setCombinerClass(TextCombiner.class);
        job.setReducerClass(TextReducer.class);

        // Specify key / value types (Don't change them for the purpose of this assignment)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //   If your mapper and combiner's  output types are different from Text.class,
        //   then uncomment the following lines to specify the data types.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    // Do not modify the main method
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TextAnalyzer(), args);
        System.exit(res);
    }
}
