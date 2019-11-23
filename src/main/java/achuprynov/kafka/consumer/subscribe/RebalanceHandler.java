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

import java.util.Collection;
import java.util.Vector;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 *
 * @author Alexander Chuprynov <achuprynov@gmail.com>
 */
public class RebalanceHandler implements ConsumerRebalanceListener {
    
    private KafkaConsumer<String, String> consumer;
    private Vector<Long> offsets;

    public RebalanceHandler(KafkaConsumer<String, String> consumer, Vector<Long> offsets) {
        this.consumer = consumer;
        this.offsets = offsets;
    }

    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for (TopicPartition partition: partitions) {
            //TODO
            System.out.println(
                    "onPartitionsRevoked for partitionNo = " + partition.partition() + 
                    " offset = " + consumer.position(partition));
        }
    }

    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        if (offsets == null) {
            for (TopicPartition partition : partitions) {
                OffsetAndMetadata metadata = consumer.committed(partition);
                int partitionNo = partition.partition();
                long offset = metadata.offset();
                
                System.out.println("stored offset for partition " + partitionNo + " = " + offset);
            }
        } else if (offsets.size() == 1 && offsets.firstElement() == 0) {
            consumer.seekToBeginning(partitions);

            for (TopicPartition partition : partitions) {
                long offset = consumer.position(partition);
                int partitionNo = partition.partition();
                
                System.out.println("first offset for partition " + partitionNo + " = " + offset);
            }
        } else {
            for (TopicPartition partition : partitions) {
                int partitionNo = partition.partition();
                Long offset;

                if (partitionNo < offsets.size()) {
                    offset = offsets.elementAt(partitionNo);
                    consumer.seek(partition, offset);
                } else {
                    offset = consumer.position(partition);
                    offset--;
                }

                System.out.println("set offset for partition " + partitionNo + " to " + offset);
            }
        }
    }
}
