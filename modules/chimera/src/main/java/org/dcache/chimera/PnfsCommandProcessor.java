/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.chimera;

import java.util.ArrayList;
import java.util.List;

public class PnfsCommandProcessor {

    public static String[] process(String command) {

        List<String> list = new ArrayList<>();
        int begin = 0;
        int deep = 0;

        for(int i = 0; i < command.length(); i++) {

            char c = command.charAt(i);
            switch(c) {
                case '(':
                    deep++;
                    if (deep == 1) {
                        begin = i+1;
                    }
                    break;
                case ')':
                    deep--;
                    if (deep == 0) {
                        list.add(command.substring(begin, i));
                    }
                    break;
            }
        }

        return list.isEmpty() ? new String[] {command} : list.toArray(new String[0]);
    }
}
