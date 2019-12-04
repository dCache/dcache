/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.util;

import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.dcache.gplazma.util.IGTFInfo.ParserException;
import org.dcache.gplazma.util.IGTFInfo.Type;
import org.dcache.util.Glob;

/**
 * A class that represents an IGTF info file.  The file
 * format is described here:
 *
 *     http://wiki.eugridpma.org/Main/IGTFInfoFile
 */
public class IGTFInfoFile
{
    private static final Logger LOG = LoggerFactory.getLogger(IGTFInfoFile.class);
    /**
     * All IGTF Policy files have filenames that match the following glob.
     */
    public static final Glob POLICY_FILENAME_GLOB = new Glob("policy-*.info");

    /** Minimum duration, in ms, to stat file. */
    private static final long CHECK_THRESHOLD = 1_000;

    private final Path file;
    private final Type type;

    private IGTFInfo policy;
    private long lastChecked;
    private FileTime lastModified;

    public IGTFInfoFile(String filename)
    {
        this(FileSystems.getDefault().getPath(filename));
    }

    public IGTFInfoFile(Path path)
    {
        file = path;
        String filename = path.getFileName().toString();
        type = POLICY_FILENAME_GLOB.matches(filename) ? Type.POLICY : Type.TRUST_ANCHOR;
    }

    public Path getPath()
    {
        return file;
    }

    public Optional<IGTFInfo> get()
    {
        if (System.currentTimeMillis() - lastChecked > CHECK_THRESHOLD) {
            lastChecked = System.currentTimeMillis();
            policy = null;
            try {
                FileTime fileLastModified = Files.getLastModifiedTime(file);
                if (lastModified == null || fileLastModified.compareTo(lastModified) > 0) {
                    policy = read(file, type);
                    lastModified = fileLastModified;
                }
            } catch (IOException | ParserException e) {
                LOG.warn("{}: {}", file.getFileName(), e.getMessage());
            }
        }

        return Optional.ofNullable(policy);
    }

    private static Collection<LogicalLine> readLines(Path path) throws IOException
    {
        List<LogicalLine> lines = new ArrayList<>();

        StringBuilder sb = null;
        int lineIndex = 0;
        int startOfLine = -1;

        for (String line : Files.readAllLines(path)) {
            lineIndex++;

            if (line.endsWith("\\")) {
                if (sb == null) {
                    sb = new StringBuilder();
                    startOfLine = lineIndex;
                }

                sb.append(line.substring(0, line.length()-1));
            } else {
                String logicalLine = trim(sb == null ? line : sb.append(line).toString());

                if (!logicalLine.isEmpty()) {
                    lines.add(new LogicalLine(logicalLine, sb == null ? lineIndex : startOfLine));
                }

                sb = null;
            }
        }

        if (sb != null) {
            String logicalLine = trim(sb.toString());
            if (!logicalLine.isEmpty()) {
                lines.add(new LogicalLine(logicalLine, startOfLine));
            }
        }

        return lines;
    }

    private static String trim(String line)
    {
        int hash = line.indexOf('#');
        if (hash > -1) {
            line = line.substring(0, hash);
        }

        return line.trim();
    }

    private static IGTFInfo read(Path path, Type type) throws IOException, ParserException
    {
        IGTFInfo.Builder builder = IGTFInfo.builder(type);
        builder.setFilename(path.getFileName().toString());

        for (LogicalLine line : readLines(path)) {
            try {
                List<String> items = Splitter.on('=').limit(2).trimResults().splitToList(line.getValue());
                if (items.size() != 2) {
                    throw new ParserException("missing '='");
                }
                String key = items.get(0);
                String value = items.get(1);

                try {
                    switch (key) {
                    case "alias":
                        builder.setAlias(value);
                        break;
                    case "version":
                        builder.setVersion(value);
                        break;
                    case "ca_url":
                        builder.setCAUrl(value);
                        break;
                    case "crl_url":
                        builder.setCRLUrl(value);
                        break;
                    case "policy_url":
                        builder.setPolicyUrl(value);
                        break;
                    case "email":
                        builder.setEmail(value);
                        break;
                    case "status":
                        builder.setStatus(value);
                        break;
                    case "url":
                        builder.setUrl(value);
                        break;
                    case "sha1fp.0":
                        builder.setSHA1FP0(value);
                        break;
                    case "subjectdn":
                        builder.setSubjectDN(value);
                        break;
                    case "requires":
                        builder.setRequires(value);
                        break;
                    case "obsoletes":
                        builder.setObsoletes(value);
                        break;
                    default:
                        throw new ParserException("unknown key");
                    }
                } catch (ParserException e) {
                    throw new ParserException("Problem with '" + key + "' line: " + e.getMessage());
                }
            } catch (ParserException e) {
                LOG.warn("{}:{} {}", path.getFileName(), line.getLineNumber(), e.getMessage());
            }
        }

        return builder.build();
    }

    /**
     * Represents a logical line: a line that may span multiple (physical)
     * lines in the file.
     */
    static class LogicalLine
    {
        private final String value;
        private final int lineNumber;

        LogicalLine(String value, int lineNumber)
        {
            this.value = value.trim();
            this.lineNumber = lineNumber;
        }

        public String getValue()
        {
            return value;
        }

        /**
         * Provide the (physical) line number where this logical line
         * started.
         */
        public int getLineNumber()
        {
            return lineNumber;
        }

        public boolean isEmpty()
        {
            return value.isEmpty();
        }
    }
}
