package com.microservices.demo.twitter.to.kafka.service.runner.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;

import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "false", matchIfMissing = true)
public class TwitterKafkaStreamRunner implements StreamRunner {

	private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);

	private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

	private final TwitterKafkaStatusListener twitterKafkaStatusListener;

	private TwitterStream twitterStream;

	public TwitterKafkaStreamRunner(TwitterToKafkaServiceConfigData configData,
			TwitterKafkaStatusListener statusListener) {
		this.twitterToKafkaServiceConfigData = configData;
		this.twitterKafkaStatusListener = statusListener;
	}

	@Override
	public void start() throws TwitterException {
		twitterStream = new TwitterStreamFactory().getInstance();
		twitterStream.addListener(twitterKafkaStatusListener);
//		addFilter();
	}

}
