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
		String topic	= args[1];
		int nthreads = Integer.parseInt( args[2] );
		
		System.out.println( Arrays.toString(args) );
		
		SingleConnectionConsumer consumer = new SingleConnectionConsumer( hostPort );
		
		for( int i=0; i < nthreads; i++ )
		{
			consumer.startListening( topic, "consumerGroup1" );
		}
	}
	
	public SingleConnectionConsumer( String hostPort )
	{
		this.hostPort = hostPort;
	}
	
	public void startListening( String topicName, String consumerGroup )
	{
		new KafkaMessageListener( hostPort, consumerGroup, topicName ).start();
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
			
			System.out.println("starting listener with threadid: " + threadid );
		}
		
		@Override
		public void run() 
		{
			KafkaConsumer<String, String>	consumer = null;
			
			Properties props = new Properties();
			props.put( "bootstrap.servers", hostPort);
			props.put( "group.id", groupId + "_" + threadid );
			props.put( "key.deserializer", StringDeserializer.class.getName());
			props.put( "value.deserializer", StringDeserializer.class.getName());
			props.put( "enable.auto.commit", true );
			
			try {
				List<String> topicList = Arrays.asList( topics );
				
				consumer = new KafkaConsumer<String, String>(props);
				consumer.subscribe( topicList );
				
				while( true )
				{
					System.out.println("waiting: " + threadid );
					ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
					System.out.println("getting messages: " + records.count() );
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
