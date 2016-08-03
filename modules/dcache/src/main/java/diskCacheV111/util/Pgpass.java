//
// $Id: Pgpass.java,v 1.2 2005-08-19 23:45:26 timur Exp $
//

package diskCacheV111.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

/**
 *
 * @author  Vladimir Podstavkov
 */
public class Pgpass {

    private final String _pwdfile;
    private String _hostname;
    private String _port;
    private String _database;
    private String _username;

    public Pgpass(String pwdfile) {
        _pwdfile = pwdfile;
    }

    private String process(String line, String hostname, String port, String database, String username) {
        if (line.charAt(0) != '#') {
//         System.out.println("process: "+line);
            String[] sa = line.split(":");
//         for (int i = 0; i < sa.length; i++) {
//             System.out.print(sa[i]+",");
//         }
//         System.out.println();
            if (sa[0].equals("*") || sa[0].equals(hostname)) {
                if (sa[1].equals("*") || sa[1].equals(port)) {
                    if (sa[2].equals("*") || sa[2].equals(database)) {
                        if (sa[3].equals("*") || sa[3].equals(username)) {
                            return sa[4];
                        }
                    }
                }
            }
        }
        return null;
    }

    private boolean parseUrl(String url) {
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
                return false;
            }
        } else {
            return false;
        }
        return true;
    }

    public String getPgpass(String hostname, String port, String database, String username) {
        //
        try {
            Process p1 = Runtime.getRuntime().exec("stat -c '%a' "+_pwdfile);
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(p1.getInputStream()));
            BufferedReader stdError = new BufferedReader(new InputStreamReader(p1.getErrorStream()));
            PrintWriter stdOutput = new PrintWriter(new BufferedWriter(new OutputStreamWriter(p1.getOutputStream())));
            String reply = stdInput.readLine();
            try {
                p1.waitFor();
            }
            catch (InterruptedException x) {
                System.out.println("stat for '"+_pwdfile+"' was interrupted");
                stdInput.close(); stdError.close(); stdOutput.close();
                return null;
            }
            stdInput.close(); stdError.close(); stdOutput.close();

            if (reply==null) {
                System.out.println("Cannot stat '"+_pwdfile+"'");
                return null;
            } else if (!reply.equals("'600'")) {
                System.out.println("Protection for '"+_pwdfile+"' must be '600'");
                return null;
            }
            /*
             * Here we can read and parse the password file
             */
            try {
                BufferedReader in = new BufferedReader(new FileReader(_pwdfile));
                String line, r = null;
                while ((line = in.readLine()) != null && r == null) {
                    r = process(line, hostname, port, database, username);
                }
                in.close();
                return r;
            } catch (IOException e) {
                System.out.println("'"+_pwdfile+"': I/O error");
                return null;
            }

        }
        catch (IOException ex) {
            System.out.println("Cannot stat "+_pwdfile);
        }
        return null;
    }

    public String getPgpass(String url, String username) {
        if (parseUrl(url)) {
            return getPgpass(_hostname, _port, _database, username);
        }
        return null;
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

    public static String getPassword(String file,
                                     String url, String user, String password)
    {
        if (file != null && !file.trim().isEmpty()) {
            Pgpass pgpass = new Pgpass(file);
            return pgpass.getPgpass(url, user);
        }
        return password;
    }
}
