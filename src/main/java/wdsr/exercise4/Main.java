package wdsr.exercise4;

import java.util.List;

import javax.jms.JMSException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import wdsr.exercise4.receiver.JmsQueueReceiver;

public class Main {
	static final String QUEUE_NAME = "MICHALJANYSZEK.QUEUE";

	private static final Logger log = LogManager.getLogger(Main.class);

	public static void main(String[] args) {

		JmsQueueReceiver receiverService = new JmsQueueReceiver(QUEUE_NAME);

		try {
			receiverService.createSession();
			List<String> messageList = receiverService.getMessage();
			for (String message : messageList) {
				log.info("Consume message: " + message);
			}
			log.info("Number of consumed messages: " + messageList.size());

			receiverService.shutdown();
		} catch (JMSException e) {
			log.error(e.getMessage());
		}

	}

}
