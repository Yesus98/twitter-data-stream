package com.microservices.demo.twitter.to.kafka.service.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.microservices.demo.config.KafkaConfigData;
import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import com.microservices.demo.kafka.producer.service.KafkaProducer;
import com.microservices.demo.twitter.to.kafka.service.transformer.TwitterStatusToAvroTransformer;

import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
public class TwitterKafkaStatusListener extends StatusAdapter {

	private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaStatusListener.class);

	private final KafkaConfigData kafkaConfigData;

	private final KafkaProducer<Long, TwitterAvroModel> kafkaProducer;

	private final TwitterStatusToAvroTransformer TwitterStatusToAvroTransformer;

	public TwitterKafkaStatusListener(KafkaConfigData configData, KafkaProducer<Long, TwitterAvroModel> producer,
			TwitterStatusToAvroTransformer transformer) {
		this.kafkaConfigData = configData;
		this.kafkaProducer = producer;
		TwitterStatusToAvroTransformer = transformer;
	}

	public void onStatus(Status status) {
		LOG.info("Received status text {} sending to kafka topic {}", status.getText(), kafkaConfigData.getTopicName());
		TwitterAvroModel twitterAvroModel = TwitterStatusToAvroTransformer.getTwitterAvroModelFromStatus(status);
		kafkaProducer.send(kafkaConfigData.getTopicName(), twitterAvroModel.getUserId(), twitterAvroModel);
	}
}
