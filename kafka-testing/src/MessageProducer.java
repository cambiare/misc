import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class MessageProducer 
{
	private KafkaProducer<String, String> producer;
	
	public static void main( String args[] )
	{
		if( args.length != 4 )
		{
			System.out.println( "USAGE: MessageProducer hostPort topic numMessages message" );
			return;
		}
		
		String 	hostPort	= args[0];
		String 	topic		= args[1];	
		int		nmsg		= Integer.parseInt( args[2] );
		String	message		= args[3];
		
		MessageProducer producer = new MessageProducer( hostPort );
		
		for( int i=0; i < nmsg; i++ )
		{
			producer.produceMessage(message + " -- #" + i, topic );
		}
		
		System.exit(0);
		
	}
	
	public MessageProducer( String hostPort )
	{
		Properties props = new Properties();
		 props.put("bootstrap.servers", hostPort);
		 props.put("acks", "all");
		 //props.put("retries", 0);
		 props.put("batch.size", 16384);
		 props.put("linger.ms", 0);
		 props.put("buffer.memory", 33554432);
		 props.put("key.serializer", StringSerializer.class.getName());
		 props.put("value.serializer", StringSerializer.class.getName());

		 System.out.println( "connecting with: " + hostPort );
		 producer = new KafkaProducer<>(props);
		 
		 producer.flush();
		 producer.close(Long.MAX_VALUE, TimeUnit.DAYS);
		 System.out.println( "done");
	}
	
	public void produceMessage( 
			String message,
			String topic )
	{
		 Future<RecordMetadata> result = producer.send(new ProducerRecord<String, String>( topic, message ) );
		 
//		 while( !result.isDone() )
//		 {
//			try {
//				Thread.sleep( 100 );
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		 }

		 System.out.println( "sent message: " + message );
	}
	
}
