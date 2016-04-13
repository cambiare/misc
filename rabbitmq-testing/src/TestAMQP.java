



import java.io.IOException;
import java.util.Date;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;

public class TestAMQP 
{
	public static final String QUEUE_NAME = "helloworldQ";
	
	public static void main( String args[] ) throws Exception
	{
		TestAMQP rabbit = new TestAMQP();
		
		int nmsg = 1;
		String action = "both";
		String msg = "no message";
		
		String host = args[1];
		String queue = QUEUE_NAME;
		
		boolean persistent = false;
		if( args[0] != null )
		{
			if( args[0].equals( "publish" ) )
			{
				action = "publish";
				msg = args[2];
				nmsg = Integer.parseInt( args[3] );
				persistent = Boolean.parseBoolean( args[4] );
				queue = args[5];
			}
			
			if( args[0].equals( "read" ) )
			{
				action = "read";
				queue = args[2];
			}
		}
		
		if( action.equals( "publish" ) )
		{	
			rabbit.publishMessages( host, msg, nmsg, persistent, queue );
		} else if( action.equals( "read" ) ) {
			rabbit.readAllMessages( host, queue );
		} else {
			System.out.println( "read or publish for first argument" );
		}
		
		
	}
	
	public void publishMessages( String host, String msg, int nmsg, boolean persistent, String queue ) throws Exception
	{
		System.out.println( "tesing" );
		
	    ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost( host );
	    factory.setUsername( "test" );
		factory.setPassword( "test" );
	    Connection connection = factory.newConnection();
	    Channel channel = connection.createChannel();
	    
	    channel.queueDeclare( queue, true, false, false, null );
	    
	    for( int i=0; i < nmsg; i++ )
	    {
	    	String tmpmsg = msg + i;
	    	BasicProperties properties = ( persistent ) ? MessageProperties.PERSISTENT_TEXT_PLAIN : null;
	    	
	    	channel.basicPublish( "", queue, properties, tmpmsg.getBytes() );
	    	
	    	if( i % 100000 == 0 )
	    	{
	    		System.out.println( new Date().toString() + " -- published " + i );
	    	}
	    	
	    }
	    
	    System.out.println( "done publishing");
	    
	    channel.close();
	    connection.close();
	    
	}
	
	public void readAllMessages( String host, String queue ) throws Exception
	{
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost( host );
		factory.setUsername( "test" );
		factory.setPassword( "test" );
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		Consumer consumer = new DefaultConsumer( channel ) {

			int counter = 0;
			
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {				
				
				counter++;
				
				if( counter % 100000 == 0 )
				{
					System.out.println( new Date().toString() + " -- read: " + counter );
				}
			}
			
		};
		
		channel.basicConsume( queue, true, consumer );
		
		
	}
}
