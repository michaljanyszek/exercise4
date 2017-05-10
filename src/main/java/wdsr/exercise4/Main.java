package wdsr.exercise4;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.sender.JmsSender;

public class Main {


	static final String TOPIC_NAME = "MICHALJANYSZEK.TOPIC";
	static final int NON_PERSISTENT_MODE = 1;
	static final int PERSISTENT_MODE = 2;
	static final int NUMBER_OF_MESSAGES = 10000;
	
	private static final Logger log = LoggerFactory.getLogger(JmsSender.class);
	public static void main(String[] args) {


		log.info("Start sending non persistent messages");
		log.info(String.format("%d non persistent messages sent in %d milliseconds", NUMBER_OF_MESSAGES, sendMessages(NON_PERSISTENT_MODE)));
		log.info("Start sending persistent messages");
		log.info(String.format("%d persistent messages sent in %d milliseconds", NUMBER_OF_MESSAGES, sendMessages(PERSISTENT_MODE)));
		
		
	}
	
	private static long sendMessages(int deliveryMode){
		JmsSender sendingService = new JmsSender(TOPIC_NAME);
		long start;
		long stop;
		long sendingTime;
		final String text = "test_";
		start = System.currentTimeMillis();
		for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
			sendingService.sendTextToTopic(String.format(text + "%d", i), deliveryMode);
		}

		stop = System.currentTimeMillis();
		sendingTime = stop - start;
		return sendingTime;

	}

}
