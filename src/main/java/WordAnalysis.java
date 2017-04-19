
import java.util.*;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class WordAnalysis {

    public static void main(String args[]){
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Word Analysis").setMaster("local[*]"));
        JavaRDD<String> lines = sc.textFile("/home/osboxes/hadoop-2.8.0/input/chap02");
        lines = lines.filter(new Function<String, Boolean>(){
            public Boolean call(String line){
                if(line == null || line.equals("") || line.equals("\n")){
                    return false;
                }
                return true;
            }
        });

        //for each word in the string, create a new tuple for a "word" and <another word , 1>
        JavaRDD<List<String>> splitLines = lines.map(new Function<String, List<String>>() {
                           public List<String> call(String line) {
                               line = line.toLowerCase();
                               String[] splitLine = line.split("[^A-Za-z0-9]+");
                               List<String> la =  Arrays.asList(splitLine);
                               return la;
                           }
                       });
        JavaPairRDD<Tuple2<String, String>, Integer> mapped = splitLines.flatMapToPair(new PairFlatMapFunction<List<String>, Tuple2<String, String>, Integer>() {
            public Iterator<Tuple2<Tuple2<String, String>, Integer>> call(List<String> strings) throws Exception {
                ArrayList<Tuple2<Tuple2<String, String>, Integer>> tups = new ArrayList<Tuple2<Tuple2<String, String>, Integer>>();
                Iterator<String> stringsIterator1 = strings.iterator();
                while(stringsIterator1.hasNext()){
                    String key = stringsIterator1.next();
                    if(key.isEmpty() || key.equals("\n") || key.equals(" ")){continue;}
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
        JavaPairRDD<Tuple2<String, String>, Integer> reduced1 = mapped.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });

       // JavaPairRDD<Tuple2<String, String>, Integer> reduced2 = mapped.reduceByKey((ax,bx)-> ax + bx);
        JavaPairRDD<String, String> kv = reduced1.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Tuple2<String, String>, Integer> orig) throws Exception {

                Tuple2<String, String> tuple2 = new Tuple2<String, String>(orig._1()._1(), "<"+ orig._1()._2() +", " + orig._2().toString() + ">");
                return tuple2;
            }
        }); //now in <ContextW, "<QueryW Int>"
        kv = kv.sortByKey(true);
        kv = kv.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s +"\n" + s2;
            }
        });

        JavaRDD<String> resultStr = kv.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> orig) throws Exception {
                return orig._1() + "\n" + orig._2() + "\n";
            }
        });
        resultStr = resultStr.sortBy(new Function<String, String>() {
            @Override
            public String call(String a){
                return a;
            }
        }, true, 1);
        resultStr.saveAsTextFile("aaaaaaaaaaa.txt");
//        int i = 1;
//        splitLines.foreach(new VoidFunction<String[]>(){
//            public void call(String[] line) {
//                if (line == null){
//                    r3eturn;
//                }
//                for(String s :line) {
//                    System.out.print(s + " ");
//                }
//                System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$");
//            }});

    }
}
