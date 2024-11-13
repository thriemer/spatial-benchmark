package de.thriemer.spatial;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SpatialOptimizationApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpatialOptimizationApplication.class, args);
    }

}
