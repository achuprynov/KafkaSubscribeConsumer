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

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 *
 * @author Alexander Chuprynov <achuprynov@gmail.com>
 */
public class CommandLineOptions extends Options {
    
    public static final String defaultBrokerAddress  = "localhost:9092";
    
    public CommandLineOptions() {
        Option optionHelp = new Option("h", "help", false, 
                "Show help (this text)");
        optionHelp.setRequired(false);
        addOption(optionHelp);
        
        Option optionBroker = new Option("b", "broker", true, 
                "Kafka broker(s) address (default - " + defaultBrokerAddress + ")");
        optionBroker.setRequired(false);
        addOption(optionBroker);
        
        Option optionTopic = new Option("t", "topic", true, 
                "Topic name");
        optionTopic.setRequired(true);
        addOption(optionTopic);
        
        Option optionOffset = new Option("o", "offsets", true, 
                "Consume data from offsets (<partition0_pos,...partitionN_pos>) (default - end of topic)");
        optionOffset.setRequired(false);
        addOption(optionOffset);
        
        Option optionGroupID = new Option("g", "group.id", true, 
                "group.id from previous consuming (default - generated automaticaly");
        optionGroupID.setRequired(false);
        addOption(optionGroupID);
    }
}
