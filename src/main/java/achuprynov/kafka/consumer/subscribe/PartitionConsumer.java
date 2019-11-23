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

import java.util.LinkedList;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 *
 * @author Alexander Chuprynov <achuprynov@gmail.com>
 */
public class PartitionConsumer extends Thread {
    
    private LinkedList<ConsumerRecord<String, String>> records;
    
    public PartitionConsumer() {
        records = new LinkedList<ConsumerRecord<String, String>>();
    }

    void push(ConsumerRecord<String, String> record) {
        synchronized(records) {
            records.add(record);
            records.notify();
        }
    }

    public void run() {
        try {
            while (true) {
                ConsumerRecord<String, String> record;

                synchronized(records) {
                    if (records.isEmpty()) {
                        records.wait();
                    }
                    record = records.remove();
                }
                
                if (record == null) {
                    break;
                }

                System.out.printf("topic = %s, partition = %d, offset = %d, message = %s%n", 
                        record.topic()
                    ,   record.partition()
                    ,   record.offset()
                    ,   record.value());
            }
        } catch (InterruptedException ex) {
            //TODO: read http://www.ibm.com/developerworks/java/library/j-jtp05236/index.html
        }
    }
}
