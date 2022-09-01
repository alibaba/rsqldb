package com.alibaba.rsqldb.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

import java.io.File;

@SpringBootApplication(scanBasePackages = {"com.alibaba.rsqldb.server"}, exclude = DataSourceAutoConfiguration.class)
public class Application {

    public static void main(String[] args) {
        if (args == null || args.length < 1) {
            throw new IllegalArgumentException("home.dir is required.");
        }
        String homeDir = args[0];
        System.setProperty("home.dir", homeDir);

        String dipperCsParentPath = homeDir + "/server";
        File file = new File(dipperCsParentPath);
        if (!file.exists()) {
            boolean result = file.mkdirs();
            if (!result) {
                throw new RuntimeException("create dipper.cs path error");
            }
        }

        System.setProperty("cs.dir", dipperCsParentPath + "/dipper.cs");

        System.out.println(System.getProperty("cs.dir"));

        SpringApplication.run(Application.class, args);
    }

}
