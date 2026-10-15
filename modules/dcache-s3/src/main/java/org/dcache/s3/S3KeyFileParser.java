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

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.dcache.util.files.LineBasedParser;

/**
 * Parses an S3 key file: one {@code <access-key> <secret>} pair per line, separated by whitespace.
 * Empty lines and lines starting with {@code #} are ignored.
 * <p>
 * Error messages never contain the line itself, as it may hold a secret.
 */
class S3KeyFileParser implements LineBasedParser<Map<String, String>> {

    private static final Splitter FIELDS =
          Splitter.on(CharMatcher.whitespace()).omitEmptyStrings();

    private final Map<String, String> keys = new HashMap<>();

    @Override
    public void accept(String line) throws UnrecoverableParsingException {
        String trimmed = line.trim();
        if (trimmed.isEmpty() || trimmed.startsWith("#")) {
            return;
        }

        List<String> fields = FIELDS.splitToList(trimmed);
        if (fields.size() != 2) {
            throw new UnrecoverableParsingException(
                  "expected \"<access-key> <secret>\", found " + fields.size() + " fields");
        }

        String accessKey = fields.get(0);
        if (keys.putIfAbsent(accessKey, fields.get(1)) != null) {
            throw new UnrecoverableParsingException("duplicate access key " + accessKey);
        }
    }

    @Override
    public Map<String, String> build() {
        return Map.copyOf(keys);
    }
}
