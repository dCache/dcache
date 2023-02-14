/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 - 2022 Deutsches Elektronen-Synchrotron
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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.lang.Nullable;

public class LoggingProducerListener<K, V> implements ProducerListener<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingProducerListener.class);

    @Override
    public void onSuccess(ProducerRecord<K, V> record, RecordMetadata recordMetadata) {
    }

    @Override
    public void onError(ProducerRecord<K, V> producerRecord,
          @Nullable RecordMetadata recordMetadata, Exception exception) {
        LOGGER.error("Producer exception occurred while publishing message : {}, exception : {}",
              producerRecord, exception);
    }


}