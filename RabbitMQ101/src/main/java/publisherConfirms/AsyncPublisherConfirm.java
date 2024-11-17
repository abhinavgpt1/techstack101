package publisherConfirms;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

class AsyncPublisherConfirm {
  private final static String QUEUE_NAME = "publisher-confirm-rmq";
  private static final int MESSAGE_COUNT = 5000;

  public static void main(String[] args) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    try (Connection connection = factory.newConnection();
        Channel channel = connection.createChannel()) {

      channel.queueDeclare(QUEUE_NAME, false, false, false, null);
      channel.confirmSelect(); // enables confirmation for message published
      ConcurrentNavigableMap<Long, String> outstandingMsgMap = new ConcurrentSkipListMap<>();

      ConfirmCallback cleanOutstandingConfirms = (sequenceNumber, multiple) -> {
          if(multiple) {
            // remove all messages from outstanding messages map which have id <= sequenceNumber
            ConcurrentNavigableMap<Long, String> confirmed = outstandingMsgMap.headMap(
                    sequenceNumber, true
            );
            confirmed.clear();
          } else {
            // remove the only occurence of sequenceNumber in outstanding messages map
            outstandingMsgMap.remove(sequenceNumber);
          }
      };
      channel.addConfirmListener(cleanOutstandingConfirms, (sequenceNumber, multiple) -> {
          // handle nack-ed messages i.e. messages that are not processed by broker by either throwing error or re-queuing. In case of nothing/error, clear the outstanding message map
        System.out.println("couldn't process message with sno.: " + sequenceNumber + ", multiple: " + multiple + ", messageBody: " + outstandingMsgMap.get(sequenceNumber));
        cleanOutstandingConfirms.handle(sequenceNumber, multiple); //IMP for "message ack check under 60sec"
      });

      // this here the configuration of queue/infra is done
      long start = System.nanoTime();

      for(int i=1; i<=MESSAGE_COUNT; i++){
        String message = "Msg: " + i;
        outstandingMsgMap.put(channel.getNextPublishSeqNo(), message);
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes()); //order is important. The published message will provide its sequence number in above statement.
        System.out.println(" [x] Sent '" + message + "'");
      }

      // Since msg ack is async, the message confirm/nack-ed can take time. So, set a limit to check all msg ack turnaround time
      if (!waitUntil(Duration.ofSeconds(60), () -> outstandingMsgMap.isEmpty())) {
        throw new IllegalStateException("All messages could not be confirmed in 60 seconds");
      }

      long end = System.nanoTime();
      System.out.format("Published %,d messages individually in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

  }
  static boolean waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
    int waited = 0;
    while (!condition.getAsBoolean() && waited < timeout.toMillis()) {
      Thread.sleep(100L);
      waited += 100;
    }
    return condition.getAsBoolean();
  }
}
/*
Output:
Published 5,000 messages individually in 1,751 ms
 */

/**
 * Ranking of execution (DESC.)
 * --------------------------
 * No publisher confirm - 1,473 ms
 * Async publisher confirm - 1,751 ms
 * Publisher confirm in batch - 2,074 ms
 * Single message publisher confirm - 12,374 ms
 */
