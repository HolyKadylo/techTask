/*
 * Saver.java
 * Illya Piven
 * DataArt's techtask
 */

package com.kadylo.app;
import com.datastax.driver.core.*;
import java.io.Serializable;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.*;


/**
 * The Saver class processes the data from Kafka topic "my-topic", parses it and saves to Cassandra.
 */
public class Saver implements Runnable {
	private static final String RECEIVER_PROPS_LOC = "/extra-resources/receiver.properties";
	private static KafkaConsumer <String, String> consumer = null;

	public void run(){
		
		/* KafkaConsumer properties */
		final Properties props = new Properties();
		System.out.println( "----->Saver started, building another Context" );
		SparkConf conf = new SparkConf ()
			.setAppName( "Saver" )
			.setMaster( "spark://appliance:7077" )
			.set( "spark.driver.allowMultipleContexts", "true" );
		JavaSparkContext sc = new JavaSparkContext ( conf );
		System.out.println( "----->Loading receiver.properties" );
		try{
			InputStream is = null;
			String propsPath = Saver.class
				.getProtectionDomain()
				.getCodeSource()
				.getLocation()
				.toURI()
				.getPath();
			propsPath = propsPath.substring(0, propsPath.lastIndexOf('/'));
			is = new FileInputStream ( propsPath + RECEIVER_PROPS_LOC );
			props.load(is);  
			System.out.println( "----->Loaded" );
		} catch (Exception e) {
			System.out.println( "----->Failed, now exit: " + e.toString());
			
			/* Since application wouldn't work properly */
			System.exit(-1);
		}

		System.out.println( "----->Creating consumer" );
		try{
			consumer = new KafkaConsumer ( props );
			consumer.subscribe(Arrays.asList("my-topic"));
		} catch (Exception e) {
			System.out.println( "----->Failed, now exit: " + e.toString());
			
			/* Again */
			System.exit(-1);
		}
		System.out.println( "----->Subscribed to my-topic" );
		System.out.println( "----->Building Cassandra cluster" );
		Cluster cluster = null;
		try {
			cluster = Cluster.builder ()                                       
				.addContactPoint( "127.0.0.1" )
				.build ();
			System.out.println( "----->Cluster built");
			Session session = cluster.connect ();   
			System.out.println( "----->Connected" );
			System.out.println( "----->Creating keyspace and table if needed" );

			session.execute("CREATE KEYSPACE IF NOT EXISTS simplex WITH replication " +
				"= {'class':'SimpleStrategy', 'replication_factor':1};");
			session.execute( "CREATE TABLE IF NOT EXISTS simplex.countries (" +
				"name text PRIMARY KEY," +
				"visits int" +
				");");

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (ConsumerRecord<String, String> record : records){
					JSONObject rec = new JSONObject(record.value());
					String country = rec.getString("country");
					int visits = rec.getInt("visits");
					System.out.println("=====>" + country + " : " + visits);

					session.execute( "INSERT INTO simplex.countries (name, visits) " +
						"VALUES (" +
						country + "," +
						visits + ")" +
						";");

				}
			}
		} catch (Exception e){
			System.out.println("----->Failed to manage input: " + e.toString());
		} finally {
			System.out.println("----->Finally closing cluster");
			if (cluster != null) cluster.close();
		}	

	}
}
