package com.microservices.demo.common.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import com.microservices.demo.config.RetryConfigData;

@Configuration
public class RetryConfig {

	private final RetryConfigData retryConfigData;

	public RetryConfig(RetryConfigData configData) {
		this.retryConfigData = configData;
	}

	@Bean
	RetryTemplate retryTemplate() {
		RetryTemplate retryTemplate = new RetryTemplate();

		/* Creamos una politica de reintento exponencial */
		ExponentialBackOffPolicy exponentialBackOffPolicy = new ExponentialBackOffPolicy();
		exponentialBackOffPolicy.setInitialInterval(retryConfigData.getInitialIntervalMs());
		exponentialBackOffPolicy.setMaxInterval(retryConfigData.getMaxIntervalMs());
		exponentialBackOffPolicy.setMultiplier(retryConfigData.getMultiplier());

		retryTemplate.setBackOffPolicy(exponentialBackOffPolicy);

		/* Creamos una politica simple de reintento */
		SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
		simpleRetryPolicy.setMaxAttempts(retryConfigData.getMaxAttempts());

		retryTemplate.setRetryPolicy(simpleRetryPolicy);

		return retryTemplate;
	}

}
