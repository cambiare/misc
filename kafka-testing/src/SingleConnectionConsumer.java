import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;


public class SingleConnectionConsumer 
{
	private String hostPort;
	private static AtomicInteger nextThreadId = new AtomicInteger();
	
	public static void main( String args[] )
	{
		String hostPort = args[0];
		
		SingleConnectionConsumer consumer = new SingleConnectionConsumer( hostPort );
		
		consumer.startListening( "topicTest1", "consumerGroup1" );
		consumer.startListening( "topicTest1", "consumerGroup1" );
		consumer.startListening( "topicTest1", "consumerGroup1" );
	}
	
	public SingleConnectionConsumer( String hostPort )
	{
		this.hostPort = hostPort;
	}
	
	public void startListening( String topicName, String consumerGroup )
	{
		new KafkaMessageListener( topicName, consumerGroup, hostPort ).start();
	}
	
	class KafkaMessageListener extends Thread
	{	
		int threadid;
		
		String groupId;
		String topics;
		String hostPort;
		
		public KafkaMessageListener( String hostPort, String groupId, String topics )
		{
			this.hostPort = hostPort;
			this.groupId = groupId;
			this.topics = topics;
			
			threadid = nextThreadId.addAndGet(1);
		}
		
		@Override
		public void run() 
		{
			KafkaConsumer<String, String>	consumer = null;
			
			Properties props = new Properties();
			props.put( "bootstrap.servers", hostPort);
			props.put( "group.id", groupId );
			props.put( "key.deserializer", StringDeserializer.class.getName());
			props.put( "value.deserializer", StringDeserializer.class.getName());
			props.put( "enable.auto.commit", true );
			
			try {
				List<String> topicList = Arrays.asList( topics );
				
				consumer = new KafkaConsumer<String, String>(props);
				consumer.subscribe( topicList );
				
				while( true )
				{
					ConsumerRecords<String, String> records = consumer.poll( 1000 );
					for( ConsumerRecord<String, String> record : records )
					{
						System.out.println( "" + threadid + ": " + 
											record.partition() + " : " + 
										    record.offset() + " : " + 
										    record.value() );
					}
				}
			} finally {
				if( consumer != null )
					consumer.close();
			}
		}
		
	}
}
