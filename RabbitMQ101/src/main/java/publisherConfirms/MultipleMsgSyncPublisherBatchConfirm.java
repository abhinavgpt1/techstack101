package publisherConfirms;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

class MultipleMsgSyncPublisherBatchConfirm {
  private final static String QUEUE_NAME = "publisher-confirm-rmq";
  private static final int MESSAGE_COUNT = 5000;
  private static final int BATCH_SIZE = 50;

  public static void main(String[] args) throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    try (Connection connection = factory.newConnection();
        Channel channel = connection.createChannel()) {

      channel.queueDeclare(QUEUE_NAME, false, false, false, null);
      channel.confirmSelect(); // enables confirmation for message published

      long start = System.nanoTime();

      int outstandingMessageCount = 0;
      for(int i=1; i<=MESSAGE_COUNT; i++){
        String message = "Msg: " + i;
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
        outstandingMessageCount++;
        if (outstandingMessageCount == BATCH_SIZE){
          // Q- what if broker wants to sends ack msg, will they not be lost if publisher keeps on sending messages?
          // A- Publisher Confirm Messages in RMQ are stored in the broker’s internal data structures
          channel.waitForConfirmsOrDie(5_000);
          outstandingMessageCount = 0;
        }
        System.out.println(" [x] Sent '" + message + "'");
      }
      if(outstandingMessageCount > 0){
        channel.waitForConfirmsOrDie(5_000);
      }
      long end = System.nanoTime();
      System.out.format("Published %,d messages individually in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
/*
Output: Published 5,000 messages individually in 2,074 ms
 */

/**
 * Ranking of execution (DESC.)
 * --------------------------
 * No publisher confirm - 1,473 ms
 * Async publisher confirm - 1,751 ms
 * Publisher confirm in batch - 2,074 ms
 * Single message publisher confirm - 12,374 ms
 */
