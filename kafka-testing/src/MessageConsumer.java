import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class MessageConsumer 
{
	public static void main( String args[] )
	{
		String	topic		= args[0];
		String	groupId		= args[1];
		String	hostPort	= args[2];
		
		MessageConsumer consumer = new MessageConsumer();
		consumer.consumeMessages( groupId, hostPort, Arrays.asList( topic ) );
	}
	
	public void consumeMessages(
			String groupId,
			String hostPort,
			List<String> topics )
	{
		KafkaConsumer<String, String>	consumer = null;
		
		Properties props = new Properties();
		props.put("bootstrap.servers", hostPort);
		props.put( "group.id", groupId );
		props.put( "key.deserializer", StringDeserializer.class.getName());
		props.put( "value.deserializer", StringDeserializer.class.getName());
		props.put( "enable.auto.commit", false );
		
		try {
		
			consumer = new KafkaConsumer<String, String>(props);
			consumer.subscribe( topics );
			
			while( true )
			{
				ConsumerRecords<String, String> records = consumer.poll( 1000 );
				for( ConsumerRecord<String, String> record : records )
				{
					System.out.println( record.partition() + " : " + 
									    record.offset() + " : " + 
									    record.value() );
					
					if( !commit( consumer, record ) )
					{
						// rollback
					}
				}
			}
		} finally {
			if( consumer != null )
				consumer.close();
		}
		
	}
	
	private boolean commit( 
			KafkaConsumer<String,String> consumer,
			ConsumerRecord<String, String> record )
	{
		try {
			consumer.commitSync(
					Collections.singletonMap( 
							new TopicPartition(
									record.topic(), record.partition() ), 
							new OffsetAndMetadata( record.offset() + 1 ) 
					) 
			);
		} catch( CommitFailedException cfe ) {
			return false;
		}
		
		return true;
	}
}
