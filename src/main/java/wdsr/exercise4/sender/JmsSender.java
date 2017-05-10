package wdsr.exercise4.sender;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsSender {
	private static final Logger log = LoggerFactory.getLogger(JmsSender.class);


	private static final String HOST="tcp://localhost:61616";
	private final String topicName;
	private Connection connection;
	private Session session;
	Destination destination;
	MessageProducer producer;
	ConnectionFactory connectionFactory;

	public JmsSender(final String topicName) {
		this.topicName = topicName;
		connectionFactory = new ActiveMQConnectionFactory(HOST);
	}

	private void connect() throws JMSException {

		connection = connectionFactory.createConnection();
		connection.start();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	}

	private void close() throws JMSException {
		session.close();
		connection.close();
	}


	public void sendTextToTopic(String text,int deliveryMode) {
		try {

			connect();
			destination = session.createTopic(topicName);
			producer = session.createProducer(destination);
			
			connection.start();
			TextMessage message = session.createTextMessage(text);
			producer.setDeliveryMode(1);
			producer.send(message);
			close();

		} catch (JMSException e) {
			e.printStackTrace();
		}
	}
}
