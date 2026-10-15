/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2026 Deutsches Elektronen-Synchrotron
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
package org.dcache.s3;

import static java.nio.file.attribute.PosixFilePermission.GROUP_READ;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_READ;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.dcache.util.Result;
import org.dcache.util.files.LineByLineParser;
import org.dcache.util.files.ParsableFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link S3KeyStore} backed by a file with one {@code <access-key> <secret>} pair per line.
 * Empty lines and lines starting with {@code #} are ignored.
 * <p>
 * The file is re-read when its modification time changes.  If any line is invalid, the whole file
 * is rejected and no access key is accepted until the file is fixed.
 * <p>
 * This class is thread-safe.
 */
public class FileS3KeyStore implements S3KeyStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileS3KeyStore.class);

    private final ParsableFile<Map<String, String>> file;

    public FileS3KeyStore(Path path) {
        var parser = new LineByLineParser<Map<String, String>>(S3KeyFileParser::new);
        file = new ParsableFile<>(p -> parse(parser, p), path);
    }

    private static Result<Map<String, String>, String> parse(
          LineByLineParser<Map<String, String>> parser, Path path) {
        warnIfReadableByOthers(path);
        return parser.apply(path);
    }

    private static void warnIfReadableByOthers(Path path) {
        try {
            Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path);
            if (permissions.contains(GROUP_READ) || permissions.contains(OTHERS_READ)) {
                LOGGER.warn("{} contains S3 secret keys but is readable by group or others",
                      path);
            }
        } catch (IOException | UnsupportedOperationException e) {
            LOGGER.debug("Unable to check permissions of {}: {}", path, e.toString());
        }
    }

    @Override
    public Optional<String> secretFor(String accessKey) {
        return file.get().getSuccess().map(keys -> keys.get(accessKey));
    }
}
