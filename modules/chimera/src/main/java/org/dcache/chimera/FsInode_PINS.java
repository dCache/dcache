/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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
package org.dcache.chimera;


import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import org.dcache.chimera.FileSystemProvider.PinInfo;

/**
 * An FsInode representing the dot command that lists pins on the targeted file, as returned by
 * PinManager.  Reading the contents of this dot-command file returns a table of all pins.  Fields
 * are separated by tab command, with missing optional values represented by a '-' character.
 */
public class FsInode_PINS extends FsInode_PGET {

    private static final Escaper REQUEST_ID_ESCAPER = Escapers.builder()
          .addEscape('"', "\"")
          .addEscape('\\', "\\")
          .build();

    private String value;

    public FsInode_PINS(FileSystemProvider fs, long ino) {
        super(fs, ino, FsInodeType.PINS);
    }

    @Override
    protected String value() throws ChimeraFsException {
        if (value == null) {
            value = buildOutput();
        }
        return value;
    }

    private String buildOutput() throws ChimeraFsException {
        List<PinInfo> pins = _fs.listPins(this);

        return pins.isEmpty() ? "" : pins.stream()
              .map(this::formatLine)
              .collect(Collectors.joining("\n", "", "\n"));
    }

    private String formatLine(PinInfo pin) {
        String displayExpiration = pin.getExpirationTime().map(Instant::toString).orElse("-");
        String displayRequestId = pin.getRequestId()
              .map(id -> "\"" + REQUEST_ID_ESCAPER.escape(id) + "\"").orElse("-");
        String displayUnpinnable = pin.isUnpinnable() ? "REMOVABLE" : "NONREMOVABLE";

        StringBuilder sb = new StringBuilder();

        sb.append(pin.getId()).append('\t');
        sb.append(pin.getCreationTime()).append('\t');
        sb.append(displayExpiration).append('\t');
        sb.append(displayRequestId).append('\t');
        sb.append(displayUnpinnable).append('\t');
        sb.append(pin.getState());

        return sb.toString();
    }
}
