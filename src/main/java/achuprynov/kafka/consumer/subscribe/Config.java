
package achuprynov.kafka.consumer.subscribe;

import java.util.Properties;
import java.util.UUID;
import java.util.Vector;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

public class Config extends Properties {
    
    private Vector<Long> offsets;
    
    public Config(String[] args) {
        CommandLineOptions options = new CommandLineOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = null;
        
        try {
            commandLine = parser.parse(options, args);
            
            if (commandLine.hasOption("help")) {
                throw new ParseException("");
            }
        } catch (ParseException ex) {
            System.out.println(ex.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("kafka.subscribe.consumer", options);
            System.exit(1);
        }
        
        if (commandLine.hasOption("broker")) {
            put("bootstrap.servers", commandLine.getOptionValue("broker"));
        } else {
            put("bootstrap.servers", CommandLineOptions.defaultBrokerAddress);
        }

        if (commandLine.hasOption("group.id")) {
            put("group.id", commandLine.getOptionValue("group.id"));
        } else {
            put("group.id", UUID.randomUUID().toString());
        }

        put("topic.name", commandLine.getOptionValue("topic"));

        if (commandLine.hasOption("offsets")) {
            String offsetsGlued = commandLine.getOptionValue("offsets");
            String[] offsetsSplitted = offsetsGlued.split(",");
            offsets = new Vector<>();

            for (String offset : offsetsSplitted) {
                offsets.add(Long.parseLong(offset));
            }
        }
        
        setKafkaOptions();
    }

    private void setKafkaOptions() {
        put("offset.store.method", "broker");
        put("enable.auto.commit",  "true");
        put("key.deserializer",    "org.apache.kafka.common.serialization.StringDeserializer");
        put("value.deserializer",  "org.apache.kafka.common.serialization.StringDeserializer");
    }
    
    public Vector<Long> getOffsets() {
        return offsets;
    }
}