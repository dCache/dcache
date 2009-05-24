package org.dcache.xrootd.security.plugins.tokenauthz;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Stack;
import java.util.StringTokenizer;


/**
 *
 * This class represents an immutable authorization envelope. For a detailed format description please refer to
 * "Authorization of data access in distributed storage systems" by Feichtinger and Peters.
 *
 *
 * @author Martin Radicke
 *
 */
public class Envelope {



    //	static Logger logger = Logger.getLogger(Envelope.class);

    /**
     * This class encapsulates all permisson and location information for one file in the grid context,
     * particularly the mapping between the logical filename (lfn) and the physical filename (transport URL,
     * TURL) as well as the access permission granted by the file catalogue.
     *
     * @author Martin Radicke
     *
     */
    public class GridFile {

        private String lfn;
        private int access;
        //		private String guid;
        //		private URL pturl;
        //		private String pguid;

        private String turlString;
        private URL turl;
        private String turlProtocol;
        private InetAddress turlHost;

        public GridFile(String lfn, String turl, String access) throws CorruptedEnvelopeException {
            this.lfn = lfn;
            this.turlString = turl;

            if (( this.access = filePermissions.indexOf(access)) == -1) {
                throw new CorruptedEnvelopeException("file permisson flag for lfn "+lfn+" must be one out of 'read', 'write-once', 'write' or 'delete'");
            }

            try {
                this.turl = parseTurl();
                this.turlHost = InetAddress.getByName(this.turl.getHost());
            } catch (MalformedURLException e) {
                throw new CorruptedEnvelopeException("Malformed TURL: "+e.getMessage());
            } catch (UnknownHostException e) {
                throw new CorruptedEnvelopeException(e.getMessage());
            }
        }

        private URL parseTurl() throws MalformedURLException {

            String rootURLString = getTurl();

            if (!rootURLString.toLowerCase().startsWith("root://")) {
                throw new MalformedURLException("TURL does not start with root://");
            }
            this.turlProtocol = "root";

            //			dirty little trick because java.net.URL does not understand root protocol but offers
            //			nice URL parsing capabilities
            rootURLString = rootURLString.replaceFirst("root","http");
            return new URL(rootURLString);
        }

        public int getAccess() {
            return access;
        }
        public String getLfn() {
            return lfn;
        }
        public String getTurl() {
            return turlString;
        }

        public String getTurlProtocol() {
            return turlProtocol;
        }
        public InetAddress getTurlHost() {
            return turlHost;
        }
        public int getTurlPort() {
            return turl.getPort();
        }
        public String getTurlPath() {
            return turl.getPath();
        }

        /**
         * Returns the username and password, if present
         * @return username OR username:password or null if no info available
         */
        public String getUserInfo() {
            return turl.getUserInfo();
        }
    }

    //	the creator of the envelope
    private String creator;

    //	UNIX timestamp specifying when envelope was created
    private long created;

    //	UNIX timestamp specifying when envelope expires
    private long expires;

    //	flag is set true if envelope was parsed successfully and token has not yet expired
    private boolean valid = false;

    //	the file information embedded in the envelope body
    List files = new LinkedList();

    private final static String ENVELOPE_START= "-----BEGIN ENVELOPE-----";
    private final static String ENVELOPE_STOP="-----END ENVELOPE-----";
    private final static String BODY_START="-----BEGIN ENVELOPE BODY-----";
    private final static String BODY_STOP="-----END ENVELOPE BODY-----";

    //	these are the possbile permission level, one file can have only one type (no combinations)
    //	the granted rights increase in the order of appereance (e.g. delete includes write, which includes read and write-once)
    public final static int READ = 0;
    public final static int WRITE_ONCE = 1;
    public final static int WRITE = 2;
    public final static int DELETE = 3;

    //	the permission level strings, used in the GridFile constructor
    private final static List filePermissions = Arrays.asList(new String[] {"read", "write-once", "write", "delete"});

    //	time frame to determine whether creatin time is still valid
    private static final long TIME_OFFSET = 60;

    //	the stack used for parsing the structured content of the envelope
    Stack stack = new Stack();


    /**
     * Parses the envelope and verifies its validity.
     * @param envelope the envelope in plain text to be parsed
     * @throws CorruptedEnvelopeException if parsing fails
     * @throws GeneralSecurityException if envelope has already expired
     */
    public Envelope(String envelope) throws CorruptedEnvelopeException, GeneralSecurityException {
        parse(envelope);
        checkValidity();
    }

    /**
     * Parses the envlope. Distiguishes between header and body.
     * @param envelope the envelope to be parsed
     * @throws CorruptedEnvelopeException if parsing fails
     */
    private void parse(String envelope) throws CorruptedEnvelopeException {

        LineNumberReader input = new LineNumberReader(new StringReader(envelope));

        try {
            String line = null;

            while ((line = input.readLine()) != null) {


                if (line.equals(ENVELOPE_START)) {
                    stack.push(ENVELOPE_START);
                    continue;
                }

                if (line.equals(ENVELOPE_STOP)) {
                    if (!stack.peek().equals(ENVELOPE_START)) {
                        throw new CorruptedEnvelopeException("Parse error near "+ENVELOPE_STOP);
                    }
                    stack.pop();
                    continue;
                }

                if (line.equals(BODY_START)) {
                    stack.push(BODY_START);
                    continue;
                }

                if (line.equals(BODY_STOP)) {
                    if (!stack.peek().equals(BODY_START)) {
                        throw new CorruptedEnvelopeException("Parse error near "+BODY_STOP);
                    }
                    stack.pop();
                    continue;
                }

                if (stack.empty() || "".equals(line)) {
                    continue;
                }


                if (stack.peek().equals(ENVELOPE_START)) {

                    //					parse a single header line
                    parseHeader(line);

                    continue;
                }

                if (stack.peek().equals(BODY_START)) {

                    //					extract xml substring
                    StringBuffer xmlBody = new StringBuffer(line);
                    while ( !(line = input.readLine()).equals("</authz>")) {
                        xmlBody.append(line);
                    }
                    xmlBody.append(line);

                    parseBody(xmlBody.toString());

                    continue;
                }

            }
        } catch (IOException e) {
            throw new CorruptedEnvelopeException("Error reading from envelope String while parsing");
        }

        try {
            input.close();
        } catch (IOException e) {
            throw new CorruptedEnvelopeException("Error closing stream where envelope string was parsed from");
        }
    }

    /**
     * Parses a single header line
     * @param line
     */
    private void parseHeader(String line) {
        StringTokenizer tokenizer = new StringTokenizer(line, ": ");

        String key = null;

        try {
            key = tokenizer.nextToken();
        } catch (NoSuchElementException e) {

            //			ignore empty line
            return;
        }

        if (key.equals("CREATOR")) {
            this.creator = tokenizer.nextToken();
            return;
        }

        if (key.equals("UNIXTIME")) {
            this.created = 	Long.parseLong(tokenizer.nextToken());
            return;
        }

        if (key.equals("EXPIRES")) {
            this.expires = 	Long.parseLong(tokenizer.nextToken());
            return;
        }

        //		logger.debug("ignoring entity "+key);

    }

    /**
     * Parses the envelope body which is expressed in XML. The body contains permission/location information
     * for at least one file.
     * @param xmlBody the substring which holds the envelope body
     * @throws CorruptedEnvelopeException if a parsing error occurs
     */
    private void parseBody(String xmlBody) throws CorruptedEnvelopeException {

        //		the XML tags to parse for
        String[] tags = {"authz","file","lfn", "turl", "access"};

        StringTokenizer tokenizer = new StringTokenizer(xmlBody, "<> ");

        String tmpLfn = null;
        String tmpTURL = null;
        String tmpPerm = null;

        while (tokenizer.hasMoreTokens()) {

            String token = tokenizer.nextToken();

            //			look for <authz>
            if (token.equals(tags[0])) {
                stack.push(tags[0]);
                continue;
            }

            //			look for </authz>
            if ((stack.peek().equals(tags[0])) && token.equals("/"+tags[0])) {
                stack.pop();
                continue;
            }

            //			look for <file>
            if (stack.peek().equals(tags[0]) && token.equals(tags[1])) {
                stack.push(tags[1]);

                //				reset temp variables for new Gridfile instance
                tmpLfn = tmpTURL = tmpPerm = null;

                continue;
            }

            //			look for </file>
            if ((stack.peek().equals(tags[1])) && token.equals("/"+tags[1])) {
                stack.pop();

                files.add(new GridFile(tmpLfn, tmpTURL, tmpPerm));
                if (!filePermissions.contains(tmpPerm)) {
                    throw new CorruptedEnvelopeException("unknown access parameter for lfn " + tmpLfn + ": "+tmpPerm);
                }
                continue;
            }

            //			look for <lfn> .. </lfn>
            if (stack.peek().equals(tags[1]) && token.equals(tags[2])) {

                tmpLfn = tokenizer.nextToken();

                if (!tokenizer.nextToken().equals("/"+tags[2])) {
                    throw new CorruptedEnvelopeException("Parse error near: "+tmpLfn);
                }
                continue;
            }

            //			look for <turl> .. </turl>
            if (stack.peek().equals(tags[1]) && token.equals(tags[3])) {

                tmpTURL = tokenizer.nextToken();

                if (!tokenizer.nextToken().equals("/"+tags[3])) {
                    throw new CorruptedEnvelopeException("Parse error near: "+tmpTURL);
                }
                continue;
            }

            //			look for <access> .. </access>
            if (stack.peek().equals(tags[1]) && token.equals(tags[4])) {

                tmpPerm = tokenizer.nextToken();

                if (!tokenizer.nextToken().equals("/"+tags[4])) {
                    throw new CorruptedEnvelopeException("Parse error near: "+tmpPerm);
                }

                continue;
            }
        }
    }

    /**
     * Checks the envelope for valid expiration date and minimum number of specified files
     * @throws GeneralSecurityException if envelope has expired
     * @throws CorruptedEnvelopeException if the number of specified files is less then 1
     */
    private void checkValidity() throws GeneralSecurityException, CorruptedEnvelopeException {

        long current = System.currentTimeMillis() / 1000;

        if ((created - TIME_OFFSET) > current) {
            throw new GeneralSecurityException("Envelope creation time lies in the future: "+new Date(created*1000));
        }

        if ((expires != 0) && (current) > expires) {
            throw new GeneralSecurityException("Envelope expired "+new Date(expires * 1000));
        }

        if (files.size() < 1 ) {
            throw new CorruptedEnvelopeException("Envelope body must contain permission and/or location information for at least one file");
        }

        valid = true;

        //		logger.debug("envelope valid");
    }


    /**
     * Gets the creation time of the envelope
     * @return the creation time as an UNIX timestamp
     */
    public long getCreationTime() {
        return created;
    }

    /**
     * Gets the creation time of the envelope formatted as Date
     * @return the creation time as Date
     */
    public Date getCreationDate() {
        return new Date(getCreationTime());
    }

    /**
     * Returns the name of the creator
     * @return the creator's name
     */
    public String getCreator() {
        return creator;
    }

    /**
     * Returns the experation time
     * @return the experation time as an UNIX timestamp or 0 if envelope never expires
     */
    public long getExpirationTime() {
        return expires;
    }

    /**
     *  Returns the expiration time formatted as Date
     * @return the expiration time as Date or null if envelope never expires
     */
    public Date getExpirationDate() {
        return expires == 0 ? null : new Date(getExpirationTime());
    }

    /**
     * Returns whether this envelope is valid or already expired
     * @return true if, and only if, the expiration date is ahead the current date
     * or if expire is equal to zero (will never expire)
     */
    public boolean isValid() {
        return valid;
    }

    /**
     * Returns access to the list of specified files
     * @return an interator the the file list
     */
    public Iterator getFiles() {
        return files.iterator();
    }
}
