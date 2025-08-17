package com.microservices.demo.twitter.to.kafka.service;

import com.microservices.demo.twitter.to.kafka.service.init.StreamInitializer;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.microservices.demo")
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

	private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);

	private final StreamRunner streamRunner;

	private final StreamInitializer streamInitializer;

	/*
	 * Es mejor usar un constructor que @Autowired, no usa reflexion y es mas
	 * eficiente
	 */
	public TwitterToKafkaServiceApplication(StreamRunner runner, StreamInitializer initializer) {
		this.streamRunner = runner;
		this.streamInitializer = initializer;
	}

	public static void main(String[] args) {
		SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		LOG.info("App starts...");
		streamInitializer.init();
		streamRunner.start();
	}
}
