package com.hiretual.search.web;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan({"com.hiretual.search.service", "com.hiretual.search.web", "com.hiretual.search.utils"})
public class ResumeSearchBackendApplication {
    public static void main(String[] args) {
        SpringApplication.run(ResumeSearchBackendApplication.class, args);
    }
}
