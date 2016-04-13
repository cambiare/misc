import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

public class MessageProducer 
{
	public static void main( String args[] )
	{
		String 	hostPort	= args[0];
		String 	topic		= args[1];	
		int		nmsg		= Integer.parseInt( args[2] );
		String	message		= args[3];
		
		MessageProducer producer = new MessageProducer();
		
		for( int i=0; i < nmsg; i++ )
		{
			message += " -- #" + i;
			producer.produceMessage(message, topic, hostPort);
		}
		
	}
	
	
	public void produceMessage( 
			String message,
			String topic,
			String hostPort )
	{
		 Properties props = new Properties();
		 props.put("bootstrap.servers", hostPort);
		 props.put("acks", "all");
		 props.put("retries", 0);
		 props.put("batch.size", 16384);
		 props.put("linger.ms", 1);
		 props.put("buffer.memory", 33554432);
		 props.put("key.serializer", StringDeserializer.class.getName());
		 props.put("value.serializer", StringDeserializer.class.getName());

		 KafkaProducer<String, String> producer = new KafkaProducer<>(props);
		 for(int i = 0; i < 100; i++)
		     producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));

		 producer.close();
	}
}
