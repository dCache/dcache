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

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.log4j.helpers.FormattingInfo;
import org.apache.log4j.helpers.PatternConverter;
import org.apache.log4j.spi.LoggingEvent;

// YYYY-MM-DDTHH:MM:SS.SSSSSSZ (or) Z can be replaced by +/-GMT
// Example: 2007-05-01T16:34:47.292-05:00
public class DatePatternConverter extends PatternConverter {

    private static final char DEFAULT_DATE_TIME_SEPARATOR = 'T';
    private static final char DEFAULT_MSECOND_SEPARATOR = '.';
    
    private Date date;
    private Calendar calendar;
    private String offset;
    private int size;
    private char separator;
    private char mSeparator;

    private static long lastTime;
    private static char[] lastTimeString = new char[20];

    public DatePatternConverter(FormattingInfo info, String params) {
        super(info);
        init(params);
    }

    protected String convert(LoggingEvent event) {
        return format(event.timeStamp);
    }

    protected void init(String params) {
        this.date = new Date();
        this.calendar = Calendar.getInstance();

        for (int i=0;i<params.length();i++) {
            char c = params.charAt(i);
            switch(c) {
            case 'M':
                if (i+1 < params.length()) {
                    this.mSeparator = params.charAt(++i);
                } else {
                    throw new IllegalArgumentException(
                            "Argument required for millisecond separator");
                }        
                break;
            case 'T':
                if (i+1 < params.length()) {
                    this.separator = params.charAt(++i);
                } else {
                    throw new IllegalArgumentException(
                            "Argument required for time separator");
                }        
                break;
            case 'z' : case 'Z':
                this.offset = "Z";
                this.calendar.setTimeZone(TimeZone.getTimeZone("UTF"));                
                this.size = 23 + this.offset.length();
                break;
            default:
                throw new IllegalArgumentException("Unknown option: " + c);
            }
        }
        
        if (this.mSeparator == 0) {
            // use default
            this.mSeparator = DatePatternConverter.DEFAULT_MSECOND_SEPARATOR;
        }
        
        if (this.separator == 0) {
            // use default
            this.separator = DatePatternConverter.DEFAULT_DATE_TIME_SEPARATOR;
        }
        
        if (this.offset == null) {
            // default to local time
            int timeOffset = (this.calendar.get(Calendar.ZONE_OFFSET) 
                              + this.calendar
                              .get(Calendar.DST_OFFSET)) / 1000;

            StringBuffer buf = new StringBuffer(6);

            if (timeOffset < 0) {
                buf.append('-');
                timeOffset = -timeOffset;
            } else {
                buf.append('+');
            }

            int hours = timeOffset / 3600;
            if (hours < 10) {
                buf.append('0');
            }
            buf.append(String.valueOf(hours));
            buf.append(':');

            int minutes = (timeOffset % 3600) / 60;
            if (minutes < 10) {
                buf.append('0');
            }
            buf.append(String.valueOf(minutes));

            this.offset = buf.toString();
            this.size = 23 + this.offset.length();
        }       
    }

    public String format(long time) {

        StringBuffer sbuf = new StringBuffer(this.size);

        int millis = (int) (time % 1000);

        if ((time - millis) != lastTime) {

            this.date.setTime(time);
            this.calendar.setTime(this.date);

            int year = this.calendar.get(Calendar.YEAR);
            sbuf.append(year);

            String month;
            switch (this.calendar.get(Calendar.MONTH)) {
            case Calendar.JANUARY:
                month = "-01-";
                break;
            case Calendar.FEBRUARY:
                month = "-02-";
                break;
            case Calendar.MARCH:
                month = "-03-";
                break;
            case Calendar.APRIL:
                month = "-04-";
                break;
            case Calendar.MAY:
                month = "-05-";
                break;
            case Calendar.JUNE:
                month = "-06-";
                break;
            case Calendar.JULY:
                month = "-07-";
                break;
            case Calendar.AUGUST:
                month = "-08-";
                break;
            case Calendar.SEPTEMBER:
                month = "-09-";
                break;
            case Calendar.OCTOBER:
                month = "-10-";
                break;
            case Calendar.NOVEMBER:
                month = "-11-";
                break;
            case Calendar.DECEMBER:
                month = "-12-";
                break;
            default:
                month = "-NA-";
                break;
            }

            sbuf.append(month);

            int day = this.calendar.get(Calendar.DAY_OF_MONTH);
            if (day < 10) {
                sbuf.append('0');
            }
            sbuf.append(day);

            sbuf.append(this.separator);

            int hour = this.calendar.get(Calendar.HOUR_OF_DAY);
            if (hour < 10) {
                sbuf.append('0');
            }
            sbuf.append(hour);
            sbuf.append(':');

            int mins = this.calendar.get(Calendar.MINUTE);
            if (mins < 10) {
                sbuf.append('0');
            }
            sbuf.append(mins);
            sbuf.append(':');

            int secs = this.calendar.get(Calendar.SECOND);
            if (secs < 10) {
                sbuf.append('0');
            }
            sbuf.append(secs);

            sbuf.append(this.mSeparator);

            // store the time string for next time to avoid recomputation
            sbuf.getChars(0, sbuf.length(), lastTimeString, 0);
            lastTime = time - millis;
        } else {
            sbuf.append(lastTimeString);
        }

        if (millis < 100) {
            sbuf.append('0');
        }
        if (millis < 10) {
            sbuf.append('0');
        }

        sbuf.append(millis);
        sbuf.append(this.offset);

        return sbuf.toString();
    }

}
