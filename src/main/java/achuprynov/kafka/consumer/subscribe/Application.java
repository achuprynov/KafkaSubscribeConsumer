/*
 * Copyright (C) 2017 Alexander Chuprynov <achuprynov@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package achuprynov.kafka.consumer.subscribe;

import java.util.Arrays;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 *
 * @author Alexander Chuprynov <achuprynov@gmail.com>
 */
public class Application {
    
    public static void main(String[] args) {
        //TODO: use Properties functions for load configuration
        System.out.println("group.id = ");
        Config config = new Config(args);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(config);

        ConsumerRebalanceListener listener = new RebalanceHandler(kafkaConsumer, config.getOffsets());

        kafkaConsumer.subscribe(Arrays.asList(config.getProperty("topic.name")), listener);

        TopicConsumer tipicConsumer = new TopicConsumer(kafkaConsumer);

        System.out.println("group.id = " + config.getProperty("group.id"));
        System.out.println("Press Ctrl + C for STOP consuming");
        
        tipicConsumer.run();
    }

}
