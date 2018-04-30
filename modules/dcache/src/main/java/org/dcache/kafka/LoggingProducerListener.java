/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.ProducerListener;

public class LoggingProducerListener implements ProducerListener {

    private static final Logger _log = LoggerFactory.getLogger(LoggingProducerListener.class);


    @Override
    public void onSuccess(String topic, Integer partition, Object key, Object value, RecordMetadata recordMetadata) {
        //forced by interface
    }

    @Override
    public void onError(String topic, Integer partition, Object key, Object value, Exception exception) {
        _log.error("Unable to send message to topic {} on  partition {}, with key {} and value {} : {}",
                topic, partition, key, value, exception.getMessage());
    }

    @Override
    public boolean isInterestedInSuccess() {
        return false;
    }
}