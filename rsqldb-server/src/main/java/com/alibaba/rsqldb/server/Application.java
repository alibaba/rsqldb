package com.alibaba.rsqldb.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(scanBasePackages = {"com.alibaba.rsqldb.server"}, exclude = DataSourceAutoConfiguration.class)
public class Application {

    public static void main(String[] args) {
        if (args == null || args.length < 1) {
            throw new IllegalArgumentException("home.dir is required.");
        }
        String homeDir = args[0];

        System.setProperty("cs.dir", homeDir + "/server");

        System.out.println(System.getProperty("cs.dir"));

        SpringApplication.run(Application.class, args);
    }

}
