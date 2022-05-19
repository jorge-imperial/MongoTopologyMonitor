package com.mongodb.example.topologychanges;

import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.connection.ClusterDescription;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;

@SpringBootApplication
public class TopologyChangesApplication implements CommandLineRunner {

    private static Logger logger = LogManager.getLogger(TopologyChangesApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(TopologyChangesApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

        String hostName = "M-C02F13T4MD6M-276.lan";

        logger.info("Connecting to replica set");
        MongoClient mongoClient = MongoClients.create(
                MongoClientSettings.builder()
                        .applyToClusterSettings(builder ->
                                builder.hosts(Arrays.asList(
                                        new ServerAddress(hostName, 27017),
                                        new ServerAddress(hostName, 27018),
                                        new ServerAddress(hostName, 27019)
                                )))
                        .applyToClusterSettings(builder ->
                                builder.addClusterListener(new ReplicaSetTopologyListener(ReadPreference.secondaryPreferred())))
                        .build()
        );

        // Sleep, connecting every second.

        while (true) {
            ClusterDescription description = mongoClient.getClusterDescription();

            logger.info("Awake: " + description.getShortDescription());

            Thread.sleep(15000);
        }
    }
}
