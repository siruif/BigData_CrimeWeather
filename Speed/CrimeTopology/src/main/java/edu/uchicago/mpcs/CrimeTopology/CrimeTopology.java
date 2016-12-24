package edu.uchicago.mpcs.CrimeTopology;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class CrimeTopology {

	static class FilterCrimeBolt extends BaseBasicBolt {
		Pattern csvPattern;
		
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			
			super.prepare(stormConf, context);
		}

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String crime = tuple.getString(0);
			Matcher csvMatcher = csvPattern.matcher(crime);
			if(!csvMatcher.find()) {
				return;
			}
			//Source: http://stackoverflow.com/questions/1757065/java-splitting-a-comma-separated-string-but-ignoring-commas-in-quotes
			String[] tokens = crime.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
			String case_num = tokens[0];
			String date = tokens[1];
			String block = tokens[2];
			String description = tokens[4];
			String ward = tokens[10];
			
			collector.emit(new Values(case_num, Byte.parseByte(date.substring(0, 2)), Byte.parseByte(date.substring(3, 5)), Short.parseShort(date.substring(6,10)), block, description, ward));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("case_num", "month", "day", "year", "block", "description", "ward"));
		}

	}


	static class UpdateCrimeBolt extends BaseBasicBolt {
		private org.apache.hadoop.conf.Configuration conf;
		private Connection hbaseConnection;
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			try {
				conf = HBaseConfiguration.create();
			    conf.set("hbase.zookeeper.property.clientPort", "2181");
			    conf.set("hbase.zookeeper.quorum", StringUtils.join((List<String>)(stormConf.get("storm.zookeeper.servers")), ","));
			    String znParent = (String)stormConf.get("zookeeper.znode.parent");
			    if(znParent == null)
			    	znParent = new String("/hbase");
				conf.set("zookeeper.znode.parent", znParent);
				hbaseConnection = ConnectionFactory.createConnection(conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			super.prepare(stormConf, context);
		}

		@Override
		public void cleanup() {
			try {
				hbaseConnection.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// TODO Auto-generated method stub
			super.cleanup();
		}

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			Table crimeTable = null;
			
			try {
				String case_num = input.getStringByField("case_num");
				String ward_month = input.getIntegerByField("ward") + "_" + input.getIntegerByField("month");
				int day = input.getIntegerByField("day");
				int year = input.getIntegerByField("year");
				String block = input.getStringByField("block");
				String description = input.getStringByField("description");

				
				crimeTable = hbaseConnection.getTable(TableName.valueOf("siruif_crime"));
				Get getCrime = new Get(Bytes.toBytes(ward_month));
				Result crime = crimeTable.get(getCrime);
				if(crime.isEmpty())  // No crime for ward and month. Punt
					return;
				
				Increment increment = new Increment(Bytes.toBytes(ward_month));
				if(description.equals("KIDNAPPING")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("kidnapping"), 1);
				}
				if(description.equals("CONCEALED CARRY LICENSE VIOLATION")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("concealed_carry_license_violation"), 1);
				}
				if(description.equals("PUBLIC PEACE VIOLATION")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("public_peace_violation"), 1);
				}
				if(description.equals("INTIMIDATION")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("intimidation"), 1);
				}
				if(description.equals("PROSTITUTION")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("prostitution"), 1);
				}
				if(description.equals("LIQUOR LAW VIOLATION")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("liquor_law_violation"), 1);
				}
				if(description.equals("ROBBERY")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("robbery"), 1);
				}
				if(description.equals("BURGLARY")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("burglary"), 1);
				}
				if(description.equals("WEAPONS VIOLATION")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("weapons_violation"), 1);
				}
				if(description.equals("HUMAN TRAFFICKING")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("human_trafficking"), 1);
				}
				if(description.equals("OTHER NARCOTIC VIOLATION")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("other_narcotic_violation"), 1);
				}
				if(description.equals("HOMICIDE")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("homicide"), 1);
				}
				if(description.equals("OBSCENITY")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("obscenity"), 1);
				}
				if(description.equals("OTHER OFFENSE")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("other_offense"), 1);
				}
				if(description.equals("CRIMINAL DAMAGE")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("criminal_damage"), 1);
				}
				if(description.equals("THEFT")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("theft"), 1);
				}
				if(description.equals("OFFENSE INVOLVING CHILDREN")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("offense_involving_children"), 1);
				}
				if(description.equals("GAMBLING")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("gambling"), 1);
				}
				if(description.equals("PUBLIC INDECENCY")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("public_indecency"), 1);
				}
				if(description.equals("NON-CRIMINAL") || description.equals("NON-CRIMENAL (SUBJECT SPECIFIED)")|| description.equals("NON - CRIMINAL")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("non_criminal"), 1);
				}
				if(description.equals("ARSON")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("arson"), 1);
				}
				if(description.equals("NARCOTICS")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("narcotics"), 1);
				}
				if(description.equals("SEX OFFENSE")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("sex_offense"), 1);
				}
				if(description.equals("STALKING")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("stalking"), 1);
				}
				if(description.equals("INTERFERENCE WITH PUBLIC OFFICER")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("interference_with_public_officer"), 1);
				}
				if(description.equals("DECEPTIVE PRACTICE")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("deceptive_practice"), 1);
				}
				if(description.equals("BATTERY")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("battery"), 1);
				}
				if(description.equals("CRIMINAL TRESPASS")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("criminal_trespass"), 1);
				}
				if(description.equals("MOTOR VEHICLE THEFT")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("motor_vehicle_theft"), 1);
				}
				if(description.equals("ASSAULT")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("assault"), 1);
				}
				if(description.equals("CRIM SEXUAL ASSAULT")) {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("crim_sexual_assault"), 1);
				}
				
				crimeTable.increment(increment);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				if(crimeTable != null)
					try {
						crimeTable.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub

		}

	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		Map stormConf = Utils.readStormConfig();
		String zookeepers = StringUtils.join((List<String>)(stormConf.get("storm.zookeeper.servers")), ",");
		System.out.println(zookeepers);
		ZkHosts zkHosts = new ZkHosts(zookeepers);
		
		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "siruif-crime", "/siruif-crime","crime_id");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaConfig.zkServers = (List<String>)stormConf.get("storm.zookeeper.servers");
		kafkaConfig.zkRoot = "/siruif-crime";
		kafkaConfig.zkPort = 2181;
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("raw-siruif-crime", kafkaSpout, 1);
		builder.setBolt("filter-siruif-crime", new FilterCrimeBolt(), 1).shuffleGrouping("raw-siruif-crime");
		builder.setBolt("extract-siruif-ward", new UpdateCrimeBolt(), 1).fieldsGrouping("filter-siruif-crime", new Fields("ward"));
		

		Map conf = new HashMap();
		conf.put(backtype.storm.Config.TOPOLOGY_WORKERS, 2);

		if (args != null && args.length > 0) {
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}   else {
			conf.put(backtype.storm.Config.TOPOLOGY_DEBUG, true);
			LocalCluster cluster = new LocalCluster(zookeepers, 2181L);
			cluster.submitTopology("siruif-crime-topology", conf, builder.createTopology());
		} 
	} 
}
