/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
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
package org.dcache.alarms.shell;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.classic.spi.LoggingEventVO;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import dmg.cells.nucleus.CDC;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.util.Args;

public class SendAlarmCLI
{
    public static void main(String[] s) throws IOException
    {
        Args args = new Args(s);
        String msg = String.join(" ", args.getArguments());
        String cell = args.getOption("s", "user-command");
        String domain = args.getOption("d", "<na>");
        String remoteHost = args.getOption("r");
        int remotePort = args.getIntOption("p");
        String type = args.getOption("t", "");

        LoggingEvent event = getAlarmEvent(msg, cell, domain, type);


        try (Socket socket = new Socket(remoteHost, remotePort);
             ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {
            out.writeObject(LoggingEventVO.build(event));
            out.flush();
        }

        System.out.println("Sent alarm to " + remoteHost + ":" + remotePort + ".");
    }

    private static LoggingEvent getAlarmEvent(String msg, String cell, String domain, String type)
    {
        LoggerContext context = new LoggerContext();
        Logger logger = context.getLogger(SendAlarmCLI.class);
        LoggingEvent event = new LoggingEvent(Logger.class.getName(), logger, Level.ERROR, msg, null, null);
        event.setMarker(AlarmMarkerFactory.getMarker(getPredefinedAlarm(type)));
        event.setMDCPropertyMap(getMdc(cell, domain));
        return event;
    }

    private static Map<String, String> getMdc(String cell, String domain)
    {
        Map<String,String> mdc = new HashMap<>();
        mdc.put(CDC.MDC_DOMAIN, domain);
        mdc.put(CDC.MDC_CELL, cell);
        return mdc;
    }

    private static PredefinedAlarm getPredefinedAlarm(String s)
    {
        if (!s.isEmpty()) {
            try {
                return PredefinedAlarm.valueOf(s.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "If specified, the alarm type must be one "
                        + "of the following:\n"
                        + ListPredefinedTypes.getSortedList());
            }
        } else {
            return null;
        }
    }
}
