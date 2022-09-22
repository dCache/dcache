/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021-2022 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.plugins;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Program to convert an storage-authzdb plugin configuration file into an omnisession plugin
 * configuration file.
 */
public class AuthzdbToOmnisession {

    private static void fail(String why) {
        System.err.println(why);
        System.exit(1);
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            fail("Require two arguments");
        }

        Path target = FileSystems.getDefault().getPath(args[1]);

        try {
            Files.deleteIfExists(target);

            Path src = FileSystems.getDefault().getPath(args[0]);
            var lines = Files.readAllLines(src);
            System.out.println("Reading authzdb configuration from " + src);

            StringBuilder sb = new StringBuilder();
            var parser = new AuthzMapLineParser();
            for (String line : lines) {
                if (line.startsWith("version 2.")) {
                    continue;
                }

                var entry = parser.parseLine(line);

                if (entry.isEmpty()) {
                    sb.append(line);
                } else {
                    var info = entry.get();
                    sb.append("username:").append(info.getUsername());

                    if (info.isReadOnly()) {
                        sb.append(" read-only");
                    }

                    sb.append(" home:").append(info.getHome());

                    sb.append(" root:").append(info.getRoot());

                    info.getMaxUpload().ifPresent(s -> sb.append(" max-upload:").append(s));
                }

                sb.append('\n');
            }

            Files.writeString(target, sb);
            System.out.println("Omnisession configuration written to " + target);
        } catch (IOException e) {
            fail(e.toString());
        }
    }
}
