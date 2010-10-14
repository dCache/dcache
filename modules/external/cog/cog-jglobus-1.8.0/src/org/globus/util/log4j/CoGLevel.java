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

import org.apache.log4j.Level;

/**
 * This class introduces a new level level called TRACE. TRACE has lower level
 * than DEBUG.
 */
public class CoGLevel extends Level {

    static public final int TRACE_INT = Level.DEBUG_INT - 1;

    private static String TRACE_STR = "TRACE";

    public static final CoGLevel TRACE = new CoGLevel(TRACE_INT, TRACE_STR, 7);

    protected CoGLevel(int level, String strLevel, int syslogEquiv) {
        super(level, strLevel, syslogEquiv);
    }

    /**
     * Convert the string passed as argument to a level. If the conversion
     * fails, then this method returns {@link #TRACE}.
     */
    public static Level toLevel(String sArg) {
        return (Level) toLevel(sArg, CoGLevel.TRACE);
    }

    public static Level toLevel(String sArg, Level defaultValue) {

        if (sArg == null) {
            return defaultValue;
        }
        String stringVal = sArg.toUpperCase();

        if (stringVal.equals(TRACE_STR)) {
            return CoGLevel.TRACE;
        }

        return Level.toLevel(sArg, (Level) defaultValue);
    }

    public static Level toLevel(int i) throws IllegalArgumentException {
        switch (i) {
        case TRACE_INT:
            return CoGLevel.TRACE;
        }
        return Level.toLevel(i);
    }

}
