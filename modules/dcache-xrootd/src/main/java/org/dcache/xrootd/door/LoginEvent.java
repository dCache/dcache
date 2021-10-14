package org.dcache.xrootd.door;

import org.dcache.auth.LoginReply;

public class LoginEvent {

    private LoginReply _loginReply;

    public LoginEvent(LoginReply loginReply) {
        _loginReply = loginReply;
    }

    public LoginReply getLoginReply() {
        return _loginReply;
    }
}
