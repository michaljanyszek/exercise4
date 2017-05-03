package wdsr.exercise4.sender;

import java.math.BigDecimal;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.Order;

public class JmsSender {
	private static final Logger log = LoggerFactory.getLogger(JmsSender.class);
	
	private final String queueName;
	private final String topicName;
	private final ActiveMQConnectionFactory connectionFactory;
	private static int ackMode;
	private static boolean transacted;
	private MessageProducer producer;

	public JmsSender(final String queueName, final String topicName) {
		this.queueName = queueName;
		this.topicName = topicName;
		ackMode = Session.AUTO_ACKNOWLEDGE;
		connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:62616");
	}

	/**
	 * This method creates an Order message with the given parameters and sends it as an ObjectMessage to the queue.
	 * @param orderId ID of the product
	 * @param product Name of the product
	 * @param price Price of the product
	 */
	public void sendOrderToQueue(final int orderId, final String product, final BigDecimal price) {
		Connection connection;
		try {
			connection = connectionFactory.createConnection();
	        connection.start();
	        Session session = connection.createSession(transacted, ackMode);
	        Destination adminQueue = session.createQueue(queueName);
	       
	        this.producer = session.createProducer(adminQueue);
            this.producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            
            Order order = new Order(orderId, product, price);
            
            ObjectMessage message = session.createObjectMessage(order);
            
            message.setJMSType("Order");
			message.setStringProperty("WDSR-System", "OrderProcessor");
			producer.send(message);
			
			session.close();
			connection.close();
            
		} catch (JMSException e) {
			log.error(e.getMessage());
		}
	}

	/**
	 * This method sends the given String to the queue as a TextMessage.
	 * @param text String to be sent
	 */
	public void sendTextToQueue(String text) {
		Connection connection;
		try {
			connection = connectionFactory.createConnection();
	        connection.start();
	        Session session = connection.createSession(transacted, ackMode);
	        Destination adminQueue = session.createQueue(queueName);
	        
	        this.producer = session.createProducer(adminQueue);
            this.producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                    
            TextMessage message = session.createTextMessage();
            message.setText(text);
            
            message.setJMSType("Order");
			message.setStringProperty("WDSR-System", "OrderProcessor");
			producer.send(message);
			
			session.close();
			connection.close();
            
		} catch (JMSException e) {
			log.error(e.getMessage());
		}
	}

	/**
	 * Sends key-value pairs from the given map to the topic as a MapMessage.
	 * @param map Map of key-value pairs to be sent.
	 */
	public void sendMapToTopic(Map<String, String> map) {
		Connection connection;
		try {
			connection = connectionFactory.createConnection();
	        connection.start();
	        Session session = connection.createSession(transacted, ackMode);
	        Destination adminQueue = session.createTopic(topicName);
	        	       
	        this.producer = session.createProducer(adminQueue);
            this.producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            
            MapMessage message = session.createMapMessage();
            
            for (Map.Entry<String, String> entry : map.entrySet()) {
				message.setString(entry.getKey(), entry.getValue());
            }
            
            message.setJMSType("Order");
			message.setStringProperty("WDSR-System", "OrderProcessor");
			producer.send(message);
			
			session.close();
			connection.close();
            
		} catch (JMSException e) {
			log.error(e.getMessage());
		}
	}
}
