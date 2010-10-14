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
package org.globus.tools.ui.proxy;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.globus.common.CoGProperties;
import org.globus.util.ConfigUtil;

public class GridProxyProperties {

    private static final String PROPS_FILE = CoGProperties.CONFIG_FILE;
    private static final String gridPropsFile =  ConfigUtil.globus_dir + PROPS_FILE;
        
    private int hours = 24;

    private int bits = 512;

    private boolean limited = false;

    private String userCertFile = ConfigUtil.discoverUserCertLocation();

    private String userKeyFile = ConfigUtil.discoverUserKeyLocation();

    private String CACertFile = ConfigUtil.discoverCertDirLocation();

    private String proxyFile = ConfigUtil.discoverProxyLocation();

    private int credLifetime = 168;

    private int portalLifetime = 2;

    private String myproxyServer = "localhost";

    private int myproxyPort = 7512;
    
    private Properties properties = null;

    public GridProxyProperties() {
        /* load Properties */
        if (hasProperties()) {
            loadProperties();
        }
    }

    public void setHours(int hours) {
        this.hours = hours;
    }

    public int getHours() {
        return hours;
    }

    public void setBits(int bits) {
        this.bits = bits;
    }

    public int getBits() {
        return bits;
    }

    public void setLimited(boolean limited) {
        this.limited = limited;
    }

    public boolean getLimited() {
        return limited;
    }

    public void setProxyFile(String proxyFile) {
        this.proxyFile = proxyFile;
    }

    public String getProxyFile() {
        return proxyFile;
    }

    public void setUserCertFile(String userCertFile) {
        this.userCertFile = userCertFile;
    }

    public String getUserCertFile() {
        return userCertFile;
    }

    public void setUserKeyFile(String userKeyFile) {
        this.userKeyFile = userKeyFile;
    }

    public String getUserKeyFile() {
        return userKeyFile;
    }

    public void setCACertFile(String CACertFile) {
        this.CACertFile = CACertFile;
    }

    public String getCACertFile() {
        return CACertFile;
    }

    public void setPortalLifetime(int portalLifetime) {
        this.portalLifetime = portalLifetime;
    }

    public int getPortalLifetime() {
        return portalLifetime;
    }

    public void setCredLifetime(int credLifetime) {
        this.credLifetime = credLifetime;
    }

    public int getCredLifetime() {
        return credLifetime;
    }

    public void setMyproxyServer(String myproxyServer) {
        this.myproxyServer = myproxyServer;
    }

    public String getMyproxyServer() {
        return myproxyServer;
    }

    public void setMyproxyPort(int myproxyPort) {
        this.myproxyPort = myproxyPort;
    }

    public int getMyproxyPort() {
        return myproxyPort;
    }

    public boolean hasProperties() {
        File pfile = new File(gridPropsFile);
        return (pfile.exists());
    }

    public void loadProperties() {
        Properties props = new Properties();

        FileInputStream in = null;
        try {
            in = new FileInputStream(gridPropsFile);
            props.load(in);
        } catch (FileNotFoundException fnfe) {
            System.err
                    .println("loadGridProxyProperties: FileNotFoundException");
        } catch (IOException ioe) {
            System.err.println("loadGridProxyProperties: IOException error");
        } finally {
            if (in != null) {
                try { in.close(); } catch (IOException e) {}
            }
        }

        this.properties = props;
        
        this.hours = getIntValue(props, "proxy.lifetime", 24);
        this.bits = getIntValue(props, "proxy.strength", 512);

        this.limited = Boolean.valueOf(props.getProperty("limited")).booleanValue();

        this.userCertFile = getStrValue(props, "usercert", 
                                        ConfigUtil.discoverUserCertLocation());
        
        this.userKeyFile = getStrValue(props, "userkey",
                                       ConfigUtil.discoverUserKeyLocation());
        
        
        this.proxyFile = getStrValue(props, "proxy",
                                     ConfigUtil.discoverProxyLocation());
        
        this.CACertFile = getStrValue(props, "cacert",
                                      ConfigUtil.discoverCertDirLocation());
                
        this.credLifetime = getIntValue(props, "cred_lifetime", 168);
        this.portalLifetime = getIntValue(props, "portal_lifetime", 2);
        
        this.myproxyServer = getStrValue(props, "myproxy_server", "localhost");   
        this.myproxyPort = getIntValue(props, "myproxy_port", 7512);
    }

    private static String getStrValue(Properties props,
                                   String prop,
                                   String defaultValue) {
        String value = props.getProperty(prop);
        if (value != null && value.trim().length() > 0) {
            return value.trim();
        } else {
            return defaultValue;
        }
    }
    
    private static int getIntValue(Properties props, 
                                   String prop, 
                                   int defaultValue) {
        String value = props.getProperty(prop);
        if (value != null && value.trim().length() > 0) {
            return Integer.parseInt(value.trim());
        } else {
            return defaultValue;
        }
    }
    
    public boolean saveProperties() {
        Properties props = (this.properties == null) 
                                ? new Properties() : this.properties;
        props.setProperty("proxy.lifetime", String.valueOf(hours));
        props.setProperty("proxy.strength", String.valueOf(bits));
        props.setProperty("limited", String.valueOf(limited));
        props.setProperty("usercert", userCertFile);
        props.setProperty("userkey", userKeyFile);
        props.setProperty("proxy", proxyFile);
        props.setProperty("cacert", CACertFile);
        props.setProperty("cred_lifetime", String.valueOf(credLifetime));
        props.setProperty("portal_lifetime", String.valueOf(portalLifetime));
        props.setProperty("myproxy_server", myproxyServer);
        props.setProperty("myproxy_port", String.valueOf(myproxyPort));

        FileOutputStream out = null;
        try {
            out = new FileOutputStream(gridPropsFile);
            props.store(out, "GridProxyInit properties");
        } catch (FileNotFoundException fnfe) {
            System.err
                    .println("saveGridProxyProperties: FileNotFoundException");
            return false;
        } catch (IOException ioe) {
            System.err.println("saveGridProxyProperties: IOException");
            return false;
        } finally {
            if (out != null) {
                try { out.close(); } catch (IOException e) {}
            }
        }
        return true;
    }

}
