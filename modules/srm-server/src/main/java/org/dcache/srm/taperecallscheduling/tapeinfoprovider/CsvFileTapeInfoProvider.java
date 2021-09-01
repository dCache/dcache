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

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Optional;
import java.util.Scanner;

public class CsvFileTapeInfoProvider extends FilebasedTapeInfoProvider {

    private static final String FILEENDING = ".txt";

    @Override
    protected void initializeTapeInfo() {
        Optional<File> ofile = getFileIfExists(getTapeinfoDir(), FILENAME_TAPES + FILEENDING);
        if(ofile.isEmpty()) return;
        File file = ofile.get();

        HashMap<String, TapeInfo> parsed = new HashMap<>();

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

                parsed.put(name, new TapeInfo(capacity, usedSpace));
            }

            setTapeInfo(parsed);

        } catch (FileNotFoundException e) {
            LOGGER.error("Reading tape info file {} failed with error {}", file.getName(), e.getMessage());
        } catch (NumberFormatException e) {
            LOGGER.error("Reading tape info file {} failed due to wrong content with error {}", file.getName(), e.getMessage());
        }
    }

    @Override
    protected void initializeTapefileInfo() {
        Optional<File> ofile = getFileIfExists(getTapeinfoDir(), FILENAME_TAPEFILES + FILEENDING);
        if(ofile.isEmpty()) return;
        File file = ofile.get();

        HashMap<String, TapefileInfo> parsed = new HashMap<>();

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

                parsed.put(fileid, new TapefileInfo(filesize, tapename));
            }

            setTapefileInfo(parsed);

        } catch (FileNotFoundException e) {
            LOGGER.error("Reading tapefile info file {} failed with error {}", file.getAbsolutePath(), e.getMessage());
        } catch (NumberFormatException e) {
            LOGGER.error("Reading tapefile info file {} failed due to wrong content with error {}", file.getAbsolutePath(), e.getMessage());
        }
    }

    @Override
    public String describe() {
        return super.describe() + "\n  File type: csv\n";
    }

}
