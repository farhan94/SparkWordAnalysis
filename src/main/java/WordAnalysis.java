
import java.util.*;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class WordAnalysis {

    public static void main(String args[]){
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Word Analysis").setMaster("local[*]"));
        JavaRDD<String> lines = sc.textFile(args[0]);

        //remove empty, newlines, and null lines
        lines = lines.filter(new Function<String, Boolean>(){
            public Boolean call(String line){
                if(line == null || line.equals("") || line.equals("\n")){
                    return false;
                }
                return true;
            }
        });

        //splitting each line into a list of words
        JavaRDD<List<String>> splitLines = lines.map(new Function<String, List<String>>() {
                           public List<String> call(String line) {
                               line = line.toLowerCase();
                               String[] splitLine = line.split("[^A-Za-z0-9]+");
                               List<String> la =  Arrays.asList(splitLine);
                               return la;
                           }
                       });

        //Now we are going through each list and creating a Tuple that contains two values (a key and a value). The Key is another tuple that contains the context word as first value
        // and the query word as the second value, the Value is an integer that equals 1

        JavaPairRDD<Tuple2<String, String>, Integer> mapped = splitLines.flatMapToPair(new PairFlatMapFunction<List<String>, Tuple2<String, String>, Integer>() {
            public Iterator<Tuple2<Tuple2<String, String>, Integer>> call(List<String> strings) throws Exception {
                ArrayList<Tuple2<Tuple2<String, String>, Integer>> tups = new ArrayList<Tuple2<Tuple2<String, String>, Integer>>();
                Iterator<String> stringsIterator1 = strings.iterator();
                HashSet<String> usedW = new HashSet<String>();
                while(stringsIterator1.hasNext()){
                    String key = stringsIterator1.next();
                    if(key.isEmpty() || key.equals("\n") || key.equals(" ")){
                        continue;
                    }
                    if(usedW.add(key) == false){
                        continue;
                    }
                    Iterator<String> stringsIterator2 = strings.iterator();
                    int myword = 0;
                    while(stringsIterator2.hasNext()){
                        String val = stringsIterator2.next();
                        if(val.isEmpty() || val.equals("\n") || val.equals(" ")){continue;}
                        if(val.equals(key) && myword == 0){
                            myword += 1;
                            continue;
                        }
                        Tuple2<Tuple2<String, String>, Integer> tuple = new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>(key, val), 1);
                        tups.add(tuple);
                    }
                }
                return tups.iterator();
            }
        });

//        JavaRDD<Tuple2<String, Integer>> mapped2 = splitLines.flatMap(new FlatMapFunction<List<String>, Tuple2<String, Integer>>() {
//            @Override
//            public Iterator<Tuple2<String, Integer>> call(List<String> strings) throws Exception {
//                Array
//            }
//        });

        // reducing what we just mapped, to combine counts that have the same Key
        JavaPairRDD<Tuple2<String, String>, Integer> reduced1 = mapped.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });

       // JavaPairRDD<Tuple2<String, String>, Integer> reduced2 = mapped.reduceByKey((ax,bx)-> ax + bx); //lambda expression for above code

        //Now we are trying to make it so that there is only one Key (the Context Word), but we want to make a map of Tuples for the Value, that contain the qword and count for it
        //this will help when we need to sort it later

        JavaPairRDD<String, TreeMap<String, Integer>> kv = reduced1.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, TreeMap<String, Integer>>() {
            @Override
            public Tuple2<String, TreeMap<String, Integer>> call(Tuple2<Tuple2<String, String>, Integer> orig) throws Exception {
                TreeMap<String, Integer> qWordCount = new TreeMap<String, Integer>();
                qWordCount.put(orig._1()._2(), orig._2());
                Tuple2<String, TreeMap<String, Integer>> tuple2 = new Tuple2<String, TreeMap<String, Integer>>(orig._1()._1(), qWordCount);
                return tuple2;
            }
        });

        //now we will reduce, this will make it so that each context word has only one treemap of querywords and counts (basically just combining treemaps for each key)
        kv = kv.reduceByKey(new Function2<TreeMap<String, Integer>, TreeMap<String, Integer>, TreeMap<String, Integer>>(){
            @Override
            public TreeMap<String, Integer> call(TreeMap<String, Integer> s, TreeMap<String, Integer> s2) throws Exception {

                Set<String> s2Keys = s2.keySet();
                for(String key: s2Keys){
                    if(s.containsKey(key)){ //dont need this conditional, but just in case
                        int count = s.get(key);
                        count += s2.get(key);
                        s.replace(key, count);
                    }
                    else{
                        s.put(key, s2.get(key));
                    }
                }
                return s;
            }
        });
        //now in the end we want it in string form with the proper formatting for query words ("<queryword, Integer"), so we are making one giant string with all the querywords and counts
        // with proper formatting [for each context words]
        JavaRDD<String> resultStr = kv.map(new Function<Tuple2<String, TreeMap<String, Integer>>, String>() {
            @Override
            public String call(Tuple2<String, TreeMap<String, Integer>> orig) throws Exception {
                TreeMap<String, Integer> queryWords = orig._2();
                String allQW = "";
                Set<String> keys = queryWords.navigableKeySet();
                for(String key : keys){
                    allQW += "<"+key+", "+queryWords.get(key)+">\n";
                }
                return orig._1() + "\n" + allQW;
            }
        });
        //now we want to sort by the key, so that when the textfile is created, it will be in sorted order.
        resultStr = resultStr.sortBy(new Function<String, String>() {
            @Override
            public String call(String a){
                return a;
            }
        }, true, 1);
        resultStr.saveAsTextFile(args[1]); //outputs to this directory

    }
}
