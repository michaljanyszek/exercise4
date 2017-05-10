package wdsr.exercise4.receiver;

import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JmsQueueReceiver {
	private static final Logger log = LoggerFactory.getLogger(JmsQueueReceiver.class);
	private final String queueName;
	static Destination destination;
	private ActiveMQConnectionFactory connectionFactory;
	private Connection connection;
	private Session session;
	private MessageConsumer consumer;


	public JmsQueueReceiver(final String queueName) {

		this.queueName = queueName;
		connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
		connectionFactory.setTrustAllPackages(true);
	}

    public void createSession() throws JMSException {

        connection = connectionFactory.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = session.createQueue(queueName);
        connection.start();
        consumer = session.createConsumer(destination);
    }

    public List<String> getMessage() {
        List<String> messageList = new ArrayList<>();
        try {
            Message message = consumer.receive(100);
            while (message != null){
                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    messageList.add(textMessage.getText());
                }
                message = consumer.receive(100);
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
        return messageList;
    }

    public void shutdown() {
        try {
            connection.close();
            session.close();
        } catch (JMSException e) {
            System.out.print(e.getMessage());
        }
    }
}