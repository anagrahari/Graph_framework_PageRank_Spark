package com.spark.parallel.pagerank;

import scala.Tuple2;

import com.google.common.collect.Iterables;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.regex.Pattern;

public class PageRank {
	
	private static final Pattern SPACES = Pattern.compile("\\s+");

	  private static class Sum implements Function2<Double, Double, Double> {

		public Double call(Double arg0, Double arg1) throws Exception {
			// TODO Auto-generated method stub
			return arg0 + arg1;
		}
	   /* @Override
	    public Double call(Double a, Double b) {
	      return a + b;
	    }*/
	  }

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		    SparkConf sparkConf = new SparkConf().setAppName("JavaPageRank");
		    JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		    // Loads in input file. It should be in format of:
		    //     URL         neighbor URL
		    //     URL         neighbor URL
		    //     URL         neighbor URL
		    //     ...
		    JavaRDD<String> lines = ctx.textFile("C:/Users/ANKIT/workspace_spark/pagerank/test-data/test1.txt"/*args[0], 1*/);
		    System.out.println("lins " + lines);
		    // Loads all URLs from input file and initialize their neighbors.
		    JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(new PairFunction<String, String, String>() {

				public Tuple2<String, String> call(String s) throws Exception {
					System.out.println("s is " + s);
					// TODO Auto-generated method stub
					String[] parts = SPACES.split(s);
			        return new Tuple2<String, String>(parts[0], parts[1]);
				}
		     
		    }).distinct().groupByKey().cache();
		    
		   /* JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(s -> {
		        String[] parts = SPACES.split(s);
		        return new Tuple2<>(parts[0], parts[1]);
		      }).distinct().groupByKey().cache();
*/
		    // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
		    JavaPairRDD<String, Double> ranks = links.mapValues(new Function<Iterable<String>, Double>() {

				public Double call(Iterable<String> arg0) throws Exception {
					// TODO Auto-generated method stub
					return 1.0;
				}
		     /* @Override
		      public Double call(Iterable<String> rs) {
		        return 1.0;
		      }*/
		    	
		    });

		    // Calculates and updates URL ranks continuously using PageRank algorithm.
		    for (int current = 0; current < 3/*Integer.parseInt(args[1])*/; current++) {
		      // Calculates URL contributions to the rank of other URLs.
		    	System.out.println("current count " + current);
		      JavaPairRDD<String, Double> contribs = links.join(ranks).values()
		        .flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
		          
		        	public Iterator<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s) {
		            int urlCount = Iterables.size(s._1);
		            System.out.println("url count " + urlCount);
		            List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
		            for (String n : s._1) {
		            	System.out.println("results are " + n + " double " + s._2() / urlCount);
		              results.add(new Tuple2<String, Double>(n, s._2() / urlCount));
		            }
		            return results.iterator();
		          }
		      });

		      // Re-calculates URL ranks based on neighbor contributions.
		      ranks = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
		      		       
		       public Double call(Double sum) {
		    	   System.out.println("sum returned " + sum);
		          return 0.15 + sum * 0.85;
		        }
		      });
		    }

		    // Collects all URL ranks and dump them to console.
		    List<Tuple2<String, Double>> output = ranks.collect();
		    System.out.println("output " + output);
		    for (Tuple2<?,?> tuple : output) {
		        System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
		    }

		    ctx.stop();
	}

}
