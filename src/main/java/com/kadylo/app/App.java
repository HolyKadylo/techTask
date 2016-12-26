/*
 * App.java
 * Illya Piven
 * DataArt's techtask
 */
 

package com.kadylo.app;
import java.io.Serializable;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.String;
import java.util.regex.*;
import java.util.Properties;
import org.json.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.Tuple2;


/**
 * The App class processes the input file with Spark 
 * and posts messages to Kafka. Running App class requires
 * running Spark master, Spark slave, Cassandra cluster,
 * Zookeeper server and Kafka server with default configurations.
 * It also requires created Kafka topic "my-topic".
 */
public class App implements Serializable {
	
	/* How much top countries do we want to have */
	private static final int TOP_RANGE = 10;
	private static final String INPUT_LOCATION = "/extra-resources/part-00009";
	private static final String SENDER_PROPS_LOC = "/extra-resources/sender.properties";
	
	/* Counter variable that we use to handle the output loop properly */
	private static int counter = 0;
	
	/* Another variable used in order of proper handling of the output loop */
	private static Integer prevValue = null;

	public static void main (String[] args) {  
		
		/* Variable for Kafka producer properties */
		final Properties props = new Properties();
		
		/* Variable used to locate input file */
		String basePath = null; 
		System.out.println( "----->Building sender Spark context..." );
		SparkConf conf = new SparkConf()
			.setAppName("Calculator")
			.setMaster("spark://appliance:7077");
		JavaSparkContext sc = new JavaSparkContext(conf);
		System.out.println( "----->Done" );
		System.out.println( "----->Loading a file to memory..." );
		try{
			basePath = App.class
				.getProtectionDomain()
				.getCodeSource()
				.getLocation()
				.toURI()
				.getPath();
			basePath = basePath.substring(0,basePath.lastIndexOf('/'));
		} catch (Exception e) {
			basePath = "~";
			System.out.println("----->Failed to find basePath: " + e.toString());
		}

		JavaRDD <String> distFile = sc.textFile( basePath + INPUT_LOCATION );
		System.out.println( "----->File loaded, looking for countries" );   
		JavaRDD <String> countriesRDD = distFile.map(
			new Function <String, String> () {
				public String call(String line) throws Exception {
					Pattern pattern = Pattern.compile("(?<=\\,)[A-Z]{3}(?=\\,)");
					Matcher matcher = pattern.matcher(line);
					while (matcher.find()) {
						return matcher.group();
					}
					return null;
				}
			}
		);
		System.out.println( "----->Done, now calculating them" );
		JavaPairRDD <String, Integer> counts = countriesRDD.mapToPair (
			new PairFunction <String, String, Integer> () {
				public Tuple2 <String, Integer> call(String s) {
					return new Tuple2 <String, Integer> (s, Integer.valueOf(1) );
				}
			}
		);
		JavaPairRDD <String, Integer> unsortedRate = counts.reduceByKey (
			new Function2<Integer, Integer, Integer> () {
				public Integer call(Integer a, Integer b) { 
					return a + b; 
				}
			}
		);

		/* Swapping, because https://issues.apache.org/jira/browse/SPARK-3655 */
		JavaPairRDD <Integer, String> swapped = unsortedRate.mapToPair(
			new PairFunction <Tuple2 <String, Integer>, Integer, String> () {
				public Tuple2 <Integer, String> call(Tuple2 <String, Integer> i) throws Exception {
					return i.swap();
				}
			}
		);

		/* false here turnes on descending order sorting */
		JavaPairRDD <Integer, String> invSortedRate = swapped.sortByKey(false);

		/* Swapping back*/
		JavaPairRDD <String, Integer> sortedRate = invSortedRate.mapToPair(
			new PairFunction <Tuple2 <Integer, String>, String, Integer> () {
				public Tuple2 <String, Integer> call(Tuple2 <Integer, String> i) throws Exception {
					return i.swap();
				}
			}
		);
		System.out.println( "----->Now countries are calculated" );
		System.out.println( "----->Loading sender.properties" );
		try{
			
			/* Input stream to load properties file */
			InputStream is = null;
			String propsPath = App.class
				.getProtectionDomain()
				.getCodeSource()
				.getLocation()
				.toURI()
				.getPath();
			propsPath = propsPath.substring(0, propsPath.lastIndexOf('/'));
			is = new FileInputStream(propsPath + SENDER_PROPS_LOC);
			props.load(is);  
			System.out.println( "----->Loaded" );
		} catch (Exception e) {
			System.out.println( "----->Failed, now exit: " + e.toString());
			
			/* Since application wouldn't work properly */
			System.exit(-1); 
		}
		System.out.println("----->Sending messages");
		sortedRate.foreach(
			new VoidFunction <Tuple2 <String, Integer> > () {
				public void call(Tuple2<String, Integer> sr ) throws Exception {
					KafkaProducer <String, String> producer;
					ProducerRecord <String, String> rec;
					JSONObject answer = new JSONObject();
					producer = new KafkaProducer(props);
					
					/* I've made so to output top 10 positions, 
					 * not just first 10 countries
					 */
					if (counter >= TOP_RANGE && !sr._2().equals(prevValue)){
						producer.close(); 
						return;
					}
					answer.put( "country", (String)sr._1() );
					answer.put( "visits", (Integer)sr._2() );
					counter++;
					rec = new ProducerRecord <String, String> (
						"my-topic", 
						String.valueOf(counter), 
						answer.toString()
					);
					producer.send( rec );
					
					/* We need previous value to know if some countries 
					 * would share the same position
					 */
					if ( counter >= (TOP_RANGE - 1) ) {
						prevValue = sr._2();
					}
				}
			}
		);
		System.out.println( "----->All messages sent" );

		/* Stopping this context, because only one one may run 
		 * https://spark.apache.org/docs/2.0.0/api/java/org/apache/spark/SparkContext.html
		 */
		sc.stop();
		
		/* Saver is second part of the whole application */
		new Thread(new Saver()).start();
	}
}