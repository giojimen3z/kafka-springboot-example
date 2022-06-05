package com.kafkaspring.cursokafkaspring;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

@SpringBootApplication
public class CursoKafkaSpringApplication implements CommandLineRunner {

  @Autowired private KafkaTemplate<String, String> kafkaTemplate;

  private static final Logger log = LoggerFactory.getLogger(CursoKafkaSpringApplication.class);

  @KafkaListener(
      topics = "gio-topic",
      containerFactory = "listenerContainerFactory",
      groupId = "gio-group",
      properties = {"max.poll.interval.ms:4000", "max.poll.records:10"})
  public void listen(List<String> messages) {
    log.info("Start Reading Messages");
    for (String message : messages) {
      log.info("Message Received {}", message);
    }
    log.info("Batch Complete");
  }

  public static void main(String[] args) {
    SpringApplication.run(CursoKafkaSpringApplication.class, args);
  }

  @Override
  public void run(String... args) throws Exception {

    for (int i = 0; i < 100; i++) {
      kafkaTemplate.send("gio-topic", String.format("Sample Message %d", i));
    }
  }
}
