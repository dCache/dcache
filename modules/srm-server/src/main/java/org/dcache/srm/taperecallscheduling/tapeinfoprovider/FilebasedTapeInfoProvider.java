/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
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

package org.dcache.srm.taperecallscheduling.tapeinfoprovider;

import org.dcache.srm.taperecallscheduling.TapeInfo;
import org.dcache.srm.taperecallscheduling.TapefileInfo;
import org.dcache.srm.taperecallscheduling.spi.TapeInfoProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class FilebasedTapeInfoProvider implements TapeInfoProvider {

    protected static final Logger LOGGER = LoggerFactory.getLogger(FilebasedTapeInfoProvider.class);

    protected static final String FILENAME_TAPES = "tapes";
    protected static final String FILENAME_TAPEFILES = "tapefiles";

    private String tapeinfoDir;

    private HashMap<String, TapeInfo> tapeInfo = new HashMap<>();
    private HashMap<String, TapefileInfo> tapeFileInfo = new HashMap<>();

    public void setTapeInfoDir(String tapeinfoDir) {
        this.tapeinfoDir = tapeinfoDir.trim().endsWith("/") ? tapeinfoDir: tapeinfoDir + "/";
    }

    protected String getTapeinfoDir() {
        return tapeinfoDir;
    }

    protected void setTapeInfo(HashMap<String, TapeInfo> info) {
        tapeInfo = info;
    }

    protected void setTapefileInfo(HashMap<String, TapefileInfo> info) {
        tapeFileInfo = info;
    }

    @Override
    public synchronized Map<String, TapeInfo> getTapeInfos(List<String> tapes) {
        if(tapeInfo.isEmpty()) {
            initializeTapeInfo();
        }
        return tapeInfo.entrySet().stream()
                .filter(e -> tapes.contains(e.getKey()))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    }

    @Override
    public synchronized Map<String, TapefileInfo> getTapefileInfos(List<String> fileids) {
        if(tapeFileInfo.isEmpty()) {
            initializeTapefileInfo();
        }
        return tapeFileInfo.entrySet()
                .stream()
                .filter(e -> fileids.contains(e.getKey()))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    }

    protected abstract void initializeTapeInfo();

    protected abstract void initializeTapefileInfo();

    protected static Optional<File> getFileIfExists(String dir, String name) {
        File file = new File(dir, name);

        if(!file.isFile() || !file.canRead() ) {
            LOGGER.error("File is not accessible: {}", file.getPath());
            return Optional.empty();
        }
        return Optional.of(file);
    }

    @Override
    public synchronized boolean reload() {
        tapeInfo.clear();
        tapeFileInfo.clear();
        return true;
    }

    @Override
    public String describe() {
        StringBuilder sb = new StringBuilder();
        sb.append("Filebased tape info provider:").append("\n");
        sb.append("  Cached tape infos: ").append(tapeInfo.size()).append("\n");
        sb.append("  Cached tapefile infos: ").append(tapeFileInfo.size());
        return sb.toString();
    }

}
