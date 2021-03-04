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

package org.dcache.srm.taperecallscheduling;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.CommandException;
import dmg.util.command.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class FilebasedTapeInfoProvider implements TapeInfoProvider, CellMessageReceiver, CellInfoProvider, CellCommandListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilebasedTapeInfoProvider.class);

    private static final String FILENAME_TAPES = "tapes.txt";
    private static final String FILENAME_TAPEFILES = "tapefiles.txt";

    private String tapeinfoDir;

    private HashMap<String, TapeInfo> tapeInfo = new HashMap<>();
    private HashMap<String, TapefileInfo> tapeFileInfo = new HashMap<>();

    public void setTapeInfoDir(String tapeinfoDir) {
        this.tapeinfoDir = tapeinfoDir.trim().endsWith("/") ? tapeinfoDir: tapeinfoDir + "/";
    }

    /**
     * Returns information on tapes requested by name
     * @param tapes list of tape names
     * @return tape infos
     */
    @Override
    public synchronized Map<String, TapeInfo> getTapeInfos(List<String> tapes) {
        if(tapeInfo.isEmpty()) {
            initializeTapeInfo();
        }
        return tapeInfo.entrySet().stream()
                .filter(e -> tapes.contains(e.getKey()))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    }

    /**
     * Returns information on a file's tape location requested by name
     * @param fileids list of files requested by fileid (pnfsid)
     * @return tapefile infos
     */
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

    private void initializeTapeInfo() {
        File file = new File(tapeinfoDir, FILENAME_TAPES);

        if(!file.isFile() || !file.canRead() ) {
            LOGGER.error("Tape info file is not accessible: {}", file.getPath());
            return;
        }

        String name;
        long capacity;
        long usedSpace;

        try (Scanner myReader = new Scanner(file)) {

            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();

                String[] lineParts = data.trim().split(",");
                if (lineParts.length!=3) {
                    LOGGER.error("Tape info file line incomplete: '{}'", data);
                    return;
                }
                name = lineParts[0];
                capacity = Long.parseLong(lineParts[1].replaceAll("\\.", ""));
                usedSpace = Long.parseLong(lineParts[2].replaceAll("\\.", ""));

                tapeInfo.put(name, new TapeInfo(capacity, usedSpace));
            }
        } catch (FileNotFoundException e) {
            LOGGER.error("Reading tape info file {} failed with error {}", file.getName(), e.getMessage());
        } catch (NumberFormatException e) {
            LOGGER.error("Reading tape info file {} failed due to wrong content with error {}", file.getName(), e.getMessage());
        }

        LOGGER.info("Tape info cache initialized: {} entries added", tapeInfo.size());
    }

    private void initializeTapefileInfo() {
        File file = new File(tapeinfoDir, FILENAME_TAPEFILES);

        if(!file.isFile() || !file.canRead() ) {
            LOGGER.error("Tapefile info file is not accessible: {}", file.getPath());
            return;
        }

        String fileid; // the srm file url
        long filesize; // KB
        String tapename;

        try (Scanner myReader = new Scanner(file)) {

            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();

                String[] lineParts = data.trim().split(",");
                if (lineParts.length!=3) {
                    LOGGER.error("Tape info file line incomplete: '{}'", data);
                    return;
                }
                fileid = lineParts[0];
                filesize = Long.parseLong(lineParts[1].replaceAll("\\.", ""));
                tapename = lineParts[2];

                tapeFileInfo.put(fileid, new TapefileInfo(filesize, tapename));
            }

        } catch (FileNotFoundException e) {
            LOGGER.error("Reading tapefile info file {} failed with error {}", file.getAbsolutePath(), e.getMessage());
        } catch (NumberFormatException e) {
            LOGGER.error("Reading tapefile info file {} failed due to wrong content with error {}", file.getAbsolutePath(), e.getMessage());
        }

        LOGGER.info("Tapefile info cache initialized: {} entries added", tapeFileInfo.size());
    }

    @Command(name = "trs reload tape info",
            hint = "The tape recall scheduler reloads the tape location information files on the next run")
    public class ReloadTapeInfoCommand implements Callable<String> {

        @Override
        public synchronized String call() throws InterruptedException, NoRouteToCellException, CommandException {
            tapeInfo.clear();
            tapeFileInfo.clear();
            return "Tape information will be reloaded during the next run";
        }
    }

    @Override
    public synchronized void getInfo(PrintWriter pw) {
        pw.printf("Cached tape infos: %s\n", tapeInfo.size());
        pw.printf("Cached tape-file infos: %s\n", tapeFileInfo.size());
    }

}
