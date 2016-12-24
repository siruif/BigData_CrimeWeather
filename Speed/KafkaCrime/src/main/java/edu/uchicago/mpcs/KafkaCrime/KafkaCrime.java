package edu.uchicago.mpcs.KafkaCrime;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;
import org.omg.CORBA.portable.InputStream;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaCrime {
	static class Task extends TimerTask {
		Task() throws MalformedURLException {
			crimeURL = new URL("https://data.cityofchicago.org/api/views/x2n5-8w5q/rows.csv?accessType=DOWNLOAD");
		}
		@Override
		public void run() {
			try {
				// Adapted from http://hortonworks.com/hadoop-tutorial/simulating-transporting-realtime-events-stream-apache-kafka/
		        Properties props = new Properties();
		        //props.put("metadata.broker.list", "sandbox.hortonworks.com:6667");
		        //props.put("zk.connect", "localhost:2181");
		        props.put("metadata.broker.list", "hadoop-m.c.mpcs53013-2015.internal:6667");
		        props.put("zk.connect", "hadoop-w-1.c.mpcs53013-2015.internal:2181,hadoop-w-0.c.mpcs53013-2015.internal:2181,hadoop-m.c.mpcs53013-2015.internal:2181");
		        props.put("serializer.class", "kafka.serializer.StringEncoder");
		        props.put("request.required.acks", "1");

		        String TOPIC = "siruif-crime";
		        ProducerConfig config = new ProducerConfig(props);

		        Producer<String, String> producer = new Producer<String, String>(config);
				java.io.InputStream is = crimeURL.openStream();
				BufferedReader br = new BufferedReader(new InputStreamReader(is));
				
				String header = br.readLine();
				//System.out.println(header);
				String line;
				
				while((line = br.readLine()) != null) {	
					//System.out.println(line);
	                KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, line);
	               // System.out.println("Sending data to producer...");
	                producer.send(data);
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}	
		URL crimeURL;
	}
	public static void main(String[] args) {
		try {
			//System.out.println("Starting the program...");
			Timer timer = new Timer();
			//System.out.println("Setting timer...");
			timer.scheduleAtFixedRate(new Task(), 0, 7*60*60*1000);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
