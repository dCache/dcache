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

import com.google.gson.GsonBuilder;
import org.dcache.srm.taperecallscheduling.TapeInfo;
import org.dcache.srm.taperecallscheduling.TapefileInfo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class JsonFileTapeInfoProvider extends FilebasedTapeInfoProvider {

    private static final String FILEENDING = ".json";

    @Override
    protected void initializeTapeInfo() {
        String filename = FILENAME_TAPES + FILEENDING;
        Optional<File> ofile = getFileIfExists(getTapeinfoDir(), filename);
        if(ofile.isEmpty()) return;

        HashMap<String, TapeInfo> result = new HashMap<>();

        GsonBuilder builder = new GsonBuilder();
        try(Reader reader = Files.newBufferedReader(Paths.get(getTapeinfoDir(), filename))) {
            Map<String, ?> mapFromJson = builder.create().fromJson(reader, Map.class);
            Map<String, Double> tapeinfo;

            for (Map.Entry<String, ?> entry : mapFromJson.entrySet()) {
                tapeinfo = (Map<String, Double>) entry.getValue();
                Double capacity = tapeinfo.get("capacity");
                Double usedSpace = tapeinfo.get("filled");

                if (entry.getKey().isEmpty() || capacity == null || usedSpace == null) {
                    LOGGER.error("Tapes info file line incomplete: '{}'", entry);
                    return;
                }
                result.put(entry.getKey(), new TapeInfo(capacity.longValue(), usedSpace.longValue()));
            }

            setTapeInfo(result);

        } catch (FileNotFoundException e) {
            LOGGER.error("Tapes info file {} could not be found and failed with error {}", filename, e.getMessage());
        } catch (NumberFormatException e) {
            LOGGER.error("Reading tapes info file {} failed due to wrong content with error {}", filename, e.getMessage());
        } catch (IOException e) {
            LOGGER.error("Reading tapes info file {} failed with error {} {} {}", filename, e.getMessage(), e.getCause(), e.getStackTrace());
        }
    }

    @Override
    protected void initializeTapefileInfo() {
        String filename = FILENAME_TAPEFILES + FILEENDING;
        Optional<File> ofile = getFileIfExists(getTapeinfoDir(), filename);
        if(ofile.isEmpty()) return;

        HashMap<String, TapefileInfo> result = new HashMap<>();

        GsonBuilder builder = new GsonBuilder();
        try(Reader reader = Files.newBufferedReader(Paths.get(getTapeinfoDir(), filename))) {
            Map<String, ?> mapFromJson = builder.create().fromJson(reader, Map.class);

            for (Map.Entry<String, ?> entry : mapFromJson.entrySet()) {
                Map<String, ?> tapefileinfo = (Map<String, ?>) entry.getValue();
                Double filesize = (Double) tapefileinfo.get("size");
                String tapename = (String) tapefileinfo.get("tapeid");

                if (entry.getKey().isEmpty() || filesize == null || tapename == null) {
                    LOGGER.error("Tapefiles info file line incomplete: '{}'", entry);
                    return;
                }
                result.put(entry.getKey(), new TapefileInfo(filesize.longValue(), tapename));
            }

            setTapefileInfo(result);

        } catch (FileNotFoundException e) {
            LOGGER.error("Tapefile info file {} could not be found and failed with error {}", filename, e.getMessage());
        } catch (NumberFormatException e) {
            LOGGER.error("Reading tapefile info file {} failed due to wrong content with error {}", filename, e.getMessage());
        } catch (IOException e) {
            LOGGER.error("Reading tapefile info file {} failed with error error {} {} {}", filename, e.getMessage(), e.getCause(), e.getStackTrace());
        }
    }

    @Override
    public String describe() {
        return super.describe() + "\n  File type: JSON\n";
    }

}
