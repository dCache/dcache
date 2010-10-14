/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.util.log4j;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

/**
 * This is a simple log4j appender that ignores all log messages. 
 */
public class NullAppender extends AppenderSkeleton {
    
    private static NullAppender appender;

    public static synchronized NullAppender getInstance() {
	if (appender == null) {
	    appender = new NullAppender();
	}
	return appender;
    }

    public void close() {
    }

    public boolean requiresLayout() {
	return false;
    }

    public void append(LoggingEvent event) {
    }

}
