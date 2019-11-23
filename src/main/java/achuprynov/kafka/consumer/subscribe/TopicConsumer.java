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

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 *
 * @author Alexander Chuprynov <achuprynov@gmail.com>
 */
public class TopicConsumer implements Runnable  {
    
    private static int pollTimeoutUsec = 100;
    private Map<Integer, PartitionConsumer> partitionConsumers = new HashMap<Integer, PartitionConsumer>();
//    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    private Consumer<String, String> consumer;

    public TopicConsumer(Consumer<String, String> consumer) {
        this.consumer  = consumer;
    }

    public void run() {
        try {
            polling();
        } finally {
            shutdown();
        }
    }

    private void polling() {
        while (true) { //!closed.get())  {
            process(consumer.poll(pollTimeoutUsec));
        }
    }

    private void process(ConsumerRecords<String, String> records) {
        for (ConsumerRecord<String, String> record : records) {
            int partitionNo = record.partition();

            PartitionConsumer partitionConsumer = partitionConsumers.get(partitionNo);

            if (partitionConsumer == null) {
                partitionConsumer = new PartitionConsumer();
                partitionConsumers.put(partitionNo, partitionConsumer);
                partitionConsumer.start();
            }

            partitionConsumer.push(record);
        }
    }

    public void shutdown() {
//        closed.set(true);
        waitCloseThreads();
    }

    private void waitCloseThreads() {
        for (PartitionConsumer partitionConsumer : partitionConsumers.values()) {
            try {
                partitionConsumer.push(null);
                partitionConsumer.join();
            } catch (InterruptedException ex) {
                //TODO: read http://www.ibm.com/developerworks/java/library/j-jtp05236/index.html
            }
        }
    }
}
