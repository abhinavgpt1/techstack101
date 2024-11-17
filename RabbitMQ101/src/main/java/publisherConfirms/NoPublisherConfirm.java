package publisherConfirms;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

class NoPublisherConfirm {
  private final static String QUEUE_NAME = "publisher-confirm-rmq";
  private static final int MESSAGE_COUNT = 5000;

  public static void main(String[] args) throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    try (Connection connection = factory.newConnection();
        Channel channel = connection.createChannel()) {

      channel.queueDeclare(QUEUE_NAME, false, false, false, null);

      long start = System.nanoTime();

      for(int i=1; i<=MESSAGE_COUNT; i++){
        String message = "Msg: " + i;
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");
      }
      long end = System.nanoTime();
      System.out.format("Published %,d messages individually in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
    }
  }
}
/*
Output:
Published 5,000 messages individually in 1,473 ms
 */

/**
 * Ranking of execution (DESC.)
 * --------------------------
 * No publisher confirm - 1,473 ms
 * Async publisher confirm - 1,751 ms
 * Publisher confirm in batch - 2,074 ms
 * Single message publisher confirm - 12,374 ms
 */
