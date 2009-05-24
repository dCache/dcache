package org.dcache.xrootd.core.connection;


public class ConnectionStatus {

    private boolean isConnected = false;
    private boolean isHandShaked = false;
    private boolean isLoggedIn = false;
    private boolean isAuthenticated = false;

    public boolean isAuthenticated() {
        return isAuthenticated;
    }

    public void setAuthenticated(boolean isAuthenticated) {
        this.isAuthenticated = isAuthenticated;
    }

    public boolean isConnected() {
        return isConnected;
    }

    public void setConnected(boolean isConnected) {
        this.isConnected = isConnected;
    }

    public boolean isHandShaked() {
        return isHandShaked;
    }

    public void setHandShaked(boolean isHandShaked) {
        this.isHandShaked = isHandShaked;
    }

    public boolean isLoggedIn() {
        return isLoggedIn;
    }

    public void setLoggedIn(boolean isLoggedIn) {
        this.isLoggedIn = isLoggedIn;
    }

}
