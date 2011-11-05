//
// $Id$
//

package org.dcache.srm.util;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.SQLException;
import java.net.UnknownHostException;
/**
 *
 * @author  Vladimir Podstavkov
 */
public class Pgpass {
    private static final Logger _logger = LoggerFactory.getLogger(Pgpass.class);
    private String _pwdfile;
    private String _hostname;
    private String _port;
    private String _database;

    public Pgpass(String pwdfile) {
        _pwdfile = pwdfile;
    }

    private String process(
            String line,
            String hostname,
            String port,
            String database,
            String username) {
        if (line.charAt(0) != '#') {
//         System.out.println("process: "+line);
            String[] sa = line.split(":");
//         for (int i = 0; i < sa.length; i++) {
//             System.out.print(sa[i]+",");
//         }
//         System.out.println();
            boolean hostMatched =sa[0].equals("*") ;
            if(!hostMatched) {
                try {
                    hostMatched = Tools.sameHost(sa[0],hostname);
                } catch(UnknownHostException uhe) {
                    _logger.warn(uhe.toString());
                }
            }
            if ( hostMatched                                   &&
                 (sa[1].equals("*") || sa[1].equals(port))     &&
                 (sa[2].equals("*") || sa[2].equals(database)) &&
                 (sa[3].equals("*") || sa[3].equals(username))    ) {
                    return sa[4];
            }
        }
        return null;
    }

    private static final String legalFormats =
            "\n supported jdbc url formats:\n"+
            " jdbc:postgresql:database\n"+
            " jdbc:postgresql://host/database\n"+
            " jdbc:postgresql://host:port/database\n";

    private void parseUrl(String url) throws SQLException {
        // -jdbcUrl=jdbc:postgresql:database
        // -jdbcUrl=jdbc:postgresql://host/database
        // -jdbcUrl=jdbc:postgresql://host:port/database
        String[] r = url.split("/");
        _hostname = "localhost";
        _port = "5432";
        if (r.length==1) {
            String[] r1 = r[0].split(":");
            _database = r1[r1.length-1];
        } else if (r.length==4) {
            _database = r[r.length-1];
            String[] r1 = r[2].split(":");
            _hostname = r1[0];
            if (r1.length==2) {
                _port = r1[1];
            } else if (r1.length > 2) {
                String error = "illegal jdbc url format: "+url+legalFormats;
                _logger.error(error);
                throw new SQLException(error);
            }
        } else {
                String error = "illegal jdbc url format: "+url+legalFormats;
                _logger.error(error);
                throw new SQLException(error);
        }
    }

    public String getPgpass(
            String hostname,
            String port,
            String database,
            String username) throws SQLException {
        //
        try{
            Process p1 = Runtime.getRuntime().exec("stat -c '%a' "+_pwdfile);
            BufferedReader stdInput = new BufferedReader(
                    new InputStreamReader(p1.getInputStream()));
            BufferedReader stdError = new BufferedReader(
                    new InputStreamReader(p1.getErrorStream()));
            PrintWriter stdOutput = new PrintWriter(
                    new BufferedWriter(new OutputStreamWriter(
                    p1.getOutputStream())));
            String reply;
            try {
                reply = stdInput.readLine();
                try {
                    p1.waitFor();
                }
                catch (InterruptedException x) {
                    _logger.error("stat for '"+_pwdfile+"' was interrupted", x);
                    throw new SQLException("Cannot stat '"+_pwdfile+"'");
                }
            } finally {
                stdInput.close();
                stdError.close();
                stdOutput.close();
            }

    //             System.out.println("mode: '"+reply+"'");
            if (reply==null) {
                _logger.error("Cannot stat '"+_pwdfile+"'");
                throw new SQLException("Cannot stat '"+_pwdfile+"'");
            } else if (!reply.equals("'600'")) {
                _logger.error("Protection for '"+_pwdfile+"' must be '600'");
                throw new SQLException("Protection for '"+_pwdfile+"' must be '600'");
            }
            /*
             * Here we can read and parse the password file
             */
            BufferedReader in = new BufferedReader(new FileReader(_pwdfile));
            String r = null;
            try {
                String line;
                while ((line = in.readLine()) != null && r == null) {
                    r = process(line, hostname, port, database, username);
    //                     System.out.println("->"+r);
                }
            } finally {
                in.close();
            }
            if(r == null) {
                String error = String.format("could not get password from '%s' "+
                    "for  hostname: '%s' ,port: %s ,database: '%s' " +
                    "and username: '%s' ",_pwdfile,hostname,port,database,username);
                _logger.error(error);
                throw new SQLException(error);
            }
            return r;
        } catch (IOException ioe) {
            _logger.error("processing '"+_pwdfile+"' failed: I/O error",ioe);
            throw new SQLException("processing '"+_pwdfile+"' failed: I/O error",ioe);
        }
    }

    public String getPgpass(String url, String username) throws SQLException {
        parseUrl(url);
        return getPgpass(_hostname, _port, _database, username);
    }

    public String getHostname() {
        return _hostname;
    }

    public String getPort() {
        return _port;
    }

    public String getDatabase() {
        return _database;
    }
}
