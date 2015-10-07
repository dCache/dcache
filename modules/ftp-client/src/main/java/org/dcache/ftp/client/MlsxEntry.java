/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dcache.ftp.client;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.ftp.client.exception.FTPException;

/**
 *
 *
 *
 *
 *
 */
public class MlsxEntry
{

    private static final Logger logger =
            LoggerFactory.getLogger(MlsxEntry.class);

    private static final SimpleDateFormat dateFormatter;

    public static final String SIZE = "size";
    public static final String MODIFY = "modify";
    public static final String CREATE = "create";
    public static final String TYPE = "type";
    public static final String UNIQUE = "unique";
    public static final String PERM = "perm";
    public static final String LANG = "lang";
    public static final String MEDIA_TYPE = "media-type";
    public static final String CHARSET = "charset";
    public static final String UNIX_MODE = "unix.mode";
    public static final String UNIX_OWNER = "unix.owner";
    public static final String UNIX_GROUP = "unix.group";
    public static final String UNIX_SLINK = "unix.slink";
    public static final String UNIX_UID = "unix.uid";
    public static final String UNIX_GID = "unix.gid";
    public static final String ERROR = "error";

    public static final String TYPE_FILE = "file";
    public static final String TYPE_CDIR = "cdir";
    public static final String TYPE_PDIR = "pdir";
    public static final String TYPE_DIR = "dir";
    public static final String TYPE_SLINK = "slink";

    public static final String ERROR_OPENFAILED = "OpenFailed";
    public static final String ERROR_INVALIDLINK = "InvalidLink";

    private String fileName = null;
    private final Hashtable facts = new Hashtable();

    /**
     * Constructor for MlsxEntry.
     *
     * @param mlsxEntry
     * @throws FTPException
     */
    public MlsxEntry(String mlsxEntry) throws FTPException
    {
        this.parse(mlsxEntry);
    }

    /**
     * Method parse.
     *
     * @param mlsxEntry
     */
    private void parse(String mlsxEntry)
    {

        StringTokenizer tokenizer = new StringTokenizer(mlsxEntry, ";");

        while (tokenizer.hasMoreTokens()) {

            String token = tokenizer.nextToken();

            if (tokenizer.hasMoreTokens()) {

                //next fact
                String fact = token;
                logger.debug("fact: " + fact);
                int equalSign = fact.indexOf('=');
                String factName = fact.substring(0, equalSign).trim().toLowerCase();
                String factValue =
                        fact.substring(equalSign + 1, fact.length());

                facts.put(factName, factValue);

            } else {

                // name: trim leading space
                this.fileName = token.substring(1,
                                                token.length());
                logger.debug("name: " + fileName);

            }
        }
    }

    public void set(String factName, String factValue)
    {
        facts.put(factName, factValue);
    }

    public String getFileName()
    {
        return this.fileName;
    }

    public String get(String factName)
    {
        return (String) facts.get(factName);
    }

    public Date getDate(String factName)
    {
        Date d = null;
        synchronized (dateFormatter) {
            try {
                d = dateFormatter.parse((String) facts.get(factName));
            } catch (ParseException e) {
                d = null;
            }
        }
        return d;
    }

    public String toString()
    {
        StringBuilder buf = new StringBuilder();
        Enumeration e = facts.keys();

        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            String value = (String) facts.get(key);
            buf.append(key).append("=").append(value).append(";");
        }

        buf.append(" ").append(fileName);

        return buf.toString();
    }

    static {
        dateFormatter = new SimpleDateFormat("yyyyMMddHHmmss");
        dateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
    }
}
