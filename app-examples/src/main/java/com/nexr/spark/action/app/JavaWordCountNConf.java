package com.nexr.spark.action.app;

import com.nexr.spark.action.app.conf.JavaWordCountFmt;
import com.nexr.spark.action.app.conf.JavaWordCountSeparator;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;


public class JavaWordCountNConf {

    public static void myAppConfUsage() {
       System.out.println("spark.myapp.test.conf should be with comma separated 2 values like this. \"SQUARE,SPACE\".");
       System.out.println("First value is for formatting output result, second is for a separator in word-count.txt");
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount-NConf")
                .getOrCreate();
        SparkContext sc = spark.sparkContext();

        String myConfSepa = sc.getConf().get("spark.myapp.conf.sepa", ",");
        String[] myAppFmts = sc.getConf().get("spark.myapp.test.conf", "SQUARE,SPACE").split(myConfSepa);

        if (myAppFmts.length != 2) {
            myAppConfUsage();
            spark.stop();
            throw new Exception("Not proper confs " + myAppFmts.toString());
        }

        System.out.println("spar.myapp.test.conf Result Formatter = " + myAppFmts[0]);
        System.out.println("spar.myapp.test.conf Word Separator = " + myAppFmts[1]);

        Pattern splitPattern = Pattern.compile(JavaWordCountSeparator.getSeparator(myAppFmts[1].toUpperCase()));

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(splitPattern.split(s)).iterator());

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        List<Tuple2<String, Integer>> output = counts.collect();
        StringBuilder builder = new StringBuilder();
        builder.append("spark-app-result=");
        for (Tuple2<?,?> tuple : output) {
            builder.append(String.format(JavaWordCountFmt.getPrintOutFormat(myAppFmts[0].toUpperCase()), tuple._1(), tuple._2()));
        }
        System.out.println(builder.toString());
        spark.stop();
    }
}
