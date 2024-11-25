package com.microservices.demo.twitter.to.kafka.service.runner.impl;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.exception.TwitterToKafkaServiceException;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {

	private static final Logger LOG = LoggerFactory.getLogger(MockKafkaStreamRunner.class);

	private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
	
	private final TwitterKafkaStatusListener twitterKafkaStatusListener;
	
	private static final Random RANDOM = new Random();

	private static final String[] WORDS = new String[] { "Lorem", "ipsum", "dolor", "sit", "amet", "consectetur",
			"adipiscing", "elit", "Etiam", "eu", "mollis", "tortor", "eget", "molestie", "lectus", "Vestibulum", "et",
			"libero", "ante", "Suspendisse", "placerat", "felis" };

	private static final String tweetAsRawJson = "{" +
			"\"created_at\":\"{0}\"," +
			"\"id\":\"{1}\"," +
			"\"text\":\"{2}\"," +
			"\"user\":{\"id\":\"{3}\"}" +
			"}";
	
	private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

	public MockKafkaStreamRunner(TwitterToKafkaServiceConfigData configData, TwitterKafkaStatusListener statusListener) {
		this.twitterToKafkaServiceConfigData = configData;
		this.twitterKafkaStatusListener = statusListener;
	}

	public void start() throws TwitterException {
		String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
		int minTweetLenght = twitterToKafkaServiceConfigData.getMockMinTweetLenght();
		int maxTweetLenght = twitterToKafkaServiceConfigData.getMockMaxTweetLenght();
		long sleepTimeMs = twitterToKafkaServiceConfigData.getMockSleepMS();
		LOG.info("Starting mock filtering twitter streams for keywords {}", Arrays.toString(keywords));
		simulateTwitterStream(keywords, minTweetLenght, maxTweetLenght, sleepTimeMs);
	}
	
	private void simulateTwitterStream(String[] keywords, int minTweetLenght, int maxTweetLenght, long sleepTimeMs) {
		Executors.newSingleThreadExecutor().submit(() -> {
			try {
				while(true) {
					String formattedTweetAsRawJson = getFormattedTweet(keywords, minTweetLenght, maxTweetLenght);
					Status status = TwitterObjectFactory.createStatus(formattedTweetAsRawJson);
					twitterKafkaStatusListener.onStatus(status);
					sleep(sleepTimeMs);
				}
			} catch (TwitterException e) {
				LOG.error("Error creating twitter status!", e);
			}
		});
	}
	
	private void sleep(long sleppTomeMs) {
		try {
			Thread.sleep(sleppTomeMs);
		} catch (InterruptedException e) {
			throw new TwitterToKafkaServiceException("Error while sleeping for waiting new status to create!!");
		}
	}
	
	private String getFormattedTweet(String[] keywords, int minTweetLenght, int maxTweetLenght) {
		String[] params = new String[] {
				ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
				String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
				getRandomTweetContent(keywords, minTweetLenght, maxTweetLenght),
				String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))
		};
		return formatTweetAsJsonWithParams(params);
	}
	
	private String formatTweetAsJsonWithParams(String[] params) {
		String tweet = tweetAsRawJson;
		
		for (int i = 0; i < params.length; i++) {
			tweet = tweet.replace("{" + i + "}", params[i]);
		}
		return tweet;
	}
	
	private String getRandomTweetContent(String[] keywords, int minTweetLenght, int maxTweetLenght) {
		StringBuilder tweet = new StringBuilder();
		int tweetlenght = RANDOM.nextInt(maxTweetLenght - minTweetLenght + 1) + minTweetLenght;
		return constructRandomTweet(keywords, tweet, tweetlenght);
	}
	
	private String constructRandomTweet(String[] keywords, StringBuilder tweet, int tweetLenght) {
		for (int i = 0; i < tweetLenght; i++) {
			tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
			if (i == tweetLenght / 2) {
				tweet.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
			}
		}
		return tweet.toString().trim();
	}

}
