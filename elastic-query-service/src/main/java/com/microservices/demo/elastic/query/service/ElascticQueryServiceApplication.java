package com.microservices.demo.elastic.query.service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.microservices.demo")
public class ElascticQueryServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(ElascticQueryServiceApplication.class, args);
    }
}
