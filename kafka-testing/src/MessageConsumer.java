import java.util.Collections;
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
	public void consumeMessages(
			String groupId,
			String hostPort )
	{
		KafkaConsumer<String, String>	consumer = null;
		
		Properties props = new Properties();
		props.put("bootstrap.servers", hostPort);
		props.put( "group.id", groupId );
		props.put( "key.deserializer", StringDeserializer.class.getName());
		props.put( "value.deserializer", StringDeserializer.class.getName());
		props.put( "enable.auto.commit", false );
		
		try {
		
			consumer = new KafkaConsumer<>(props);
		
			while( true )
			{
				ConsumerRecords<String, String> records = consumer.poll( 1000 );
				for( ConsumerRecord<String, String> record : records )
				{
					record.value();
					
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
