package wdsr.exercise4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import wdsr.exercise4.sender.JmsSender;

public class Main {
	static final String QUEUE_NAME = "MICHALJANYSZEK.QUEUE";
	static final int NON_PERSISTENT_MODE = 1;
	static final int PERSISTENT_MODE = 2;
	static final int NUMBER_OF_MESSAGES = 10000;

	private static final Logger log = LogManager.getLogger(Main.class);
	
	public static void main(String[] args) {
		
		log.info("Start sending non persistent messages");
		log.info(String.format("%d non persistent messages sent in %d milliseconds", NUMBER_OF_MESSAGES, sendMessages(NON_PERSISTENT_MODE)));
		log.info("Start sending persistent messages");
		log.info(String.format("%d persistent messages sent in %d milliseconds", NUMBER_OF_MESSAGES, sendMessages(PERSISTENT_MODE)));
	}
	
	private static long sendMessages(int deliveryMode){
		JmsSender sendingService = new JmsSender(QUEUE_NAME);
		long start, stop, sendingTime;
		final String text = "test_";
		start = System.currentTimeMillis();
		for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
			sendingService.sendTextToQueue(String.format(text + "%d", i), deliveryMode);
		}

		stop = System.currentTimeMillis();
		sendingTime = stop - start;
		
		return sendingTime;
	}

}
