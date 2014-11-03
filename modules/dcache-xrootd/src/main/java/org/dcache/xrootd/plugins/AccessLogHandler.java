/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
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
package org.dcache.xrootd.plugins;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.slf4j.Logger;

import javax.security.auth.Subject;

import java.net.InetSocketAddress;
import java.security.Principal;

import dmg.cells.nucleus.CDC;

import org.dcache.auth.GidPrincipal;
import org.dcache.auth.LoginReply;
import org.dcache.auth.Subjects;
import org.dcache.auth.UidPrincipal;
import org.dcache.util.NetLoggerBuilder;
import org.dcache.xrootd.door.LoginEvent;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.protocol.messages.AbstractResponseMessage;
import org.dcache.xrootd.protocol.messages.ErrorResponse;
import org.dcache.xrootd.protocol.messages.LocateRequest;
import org.dcache.xrootd.protocol.messages.MkDirRequest;
import org.dcache.xrootd.protocol.messages.MvRequest;
import org.dcache.xrootd.protocol.messages.OpenRequest;
import org.dcache.xrootd.protocol.messages.PathRequest;
import org.dcache.xrootd.protocol.messages.PrepareRequest;
import org.dcache.xrootd.protocol.messages.ReadRequest;
import org.dcache.xrootd.protocol.messages.ReadVRequest;
import org.dcache.xrootd.protocol.messages.SetRequest;
import org.dcache.xrootd.protocol.messages.StatxRequest;
import org.dcache.xrootd.protocol.messages.WriteRequest;
import org.dcache.xrootd.protocol.messages.XrootdRequest;

import static org.dcache.util.NetLoggerBuilder.Level.DEBUG;
import static org.dcache.util.NetLoggerBuilder.Level.ERROR;
import static org.dcache.util.NetLoggerBuilder.Level.INFO;
import static org.dcache.xrootd.protocol.XrootdProtocol.*;

public class AccessLogHandler extends SimpleChannelHandler
{
    private final Logger logger;

    public AccessLogHandler(Logger logger)
    {
        this.logger = logger;
    }

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception
    {
        if (e instanceof LoginEvent) {
            LoginReply loginReply = ((LoginEvent) e).getLoginReply();
            Subject subject = loginReply.getSubject();
            NetLoggerBuilder log = new NetLoggerBuilder(INFO, "org.dcache.xrootd.login").omitNullValues();
            log.add("session", CDC.getSession());
            log.add("dn", Subjects.getDn(subject));
            log.add("user", getUser(subject));
            log.toLogger(logger);
        }
        super.handleUpstream(ctx, e);
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
    {
        NetLoggerBuilder log = new NetLoggerBuilder(INFO, "org.dcache.xrootd.connection.start").omitNullValues();
        log.add("session", CDC.getSession());
        log.add("host.remote", getAddress((InetSocketAddress) e.getChannel().getRemoteAddress()));
        log.add("host.local", getAddress((InetSocketAddress) e.getChannel().getLocalAddress()));
        log.toLogger(logger);
        super.channelConnected(ctx, e);
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
    {
        NetLoggerBuilder log = new NetLoggerBuilder(INFO, "org.dcache.xrootd.connection.end").omitNullValues();
        log.add("session", CDC.getSession());
        log.toLogger(logger);
        super.channelDisconnected(ctx, e);
    }

    @Override
    public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception
    {
        Object msg = e.getMessage();
        if (msg instanceof AbstractResponseMessage && logger.isErrorEnabled()) {
            AbstractResponseMessage response = (AbstractResponseMessage) msg;
            XrootdRequest request = response.getRequest();

            NetLoggerBuilder.Level level;
            if (response instanceof ErrorResponse) {
                level = ERROR;
            } else if (request instanceof WriteRequest || request instanceof ReadRequest || request instanceof ReadVRequest) {
                level = DEBUG;
            } else {
                level = INFO;
            }

            if (level == ERROR || level == INFO && logger.isInfoEnabled() || level == DEBUG && logger.isDebugEnabled()) {
                NetLoggerBuilder log = new NetLoggerBuilder(level, "org.dcache.xrootd.request").omitNullValues();
                log.add("session", CDC.getSession());
                log.add("request", getRequestId(request));

                if (request instanceof PathRequest) {
                    log.add("path", ((PathRequest) request).getPath());
                    if (request instanceof OpenRequest) {
                        log.add("mode", "0" + Integer.toOctalString(((OpenRequest) request).getUMask()));
                        log.add("options", "0x" + Integer.toHexString(((OpenRequest) request).getOptions()));
                    } else if (request instanceof LocateRequest) {
                        log.add("options", "0x" + Integer.toHexString(((LocateRequest) request).getOptions()));
                    } else if (request instanceof MkDirRequest) {
                        log.add("options", "0x" + Integer.toHexString(((MkDirRequest) request).getOptions()));
                    }
                } else if (request instanceof MvRequest) {
                    log.add("source", ((MvRequest) request).getSourcePath());
                    log.add("target", ((MvRequest) request).getTargetPath());
                } else if (request instanceof PrepareRequest) {
                    log.add("options", "0x" + Integer.toHexString(((PrepareRequest) request).getOptions()));
                    if (((PrepareRequest) request).getPathList().length == 1) {
                        log.add("path", ((PrepareRequest) request).getPathList()[0]);
                    } else {
                        log.add("files", ((PrepareRequest) request).getPathList().length);
                    }
                } else if (request instanceof StatxRequest) {
                    if (((StatxRequest) request).getPaths().length == 1) {
                        log.add("path", ((StatxRequest) request).getPaths()[0]);
                    } else {
                        log.add("files", ((StatxRequest) request).getPaths().length);
                    }
                } else if (request instanceof SetRequest) {
                    final String APPID_PREFIX = "appid ";
                    final int APPID_PREFIX_LENGTH = APPID_PREFIX.length();
                    final int APPID_MSG_LENGTH = 80;
                    String data = ((SetRequest) request).getData();
                    if (data.startsWith(APPID_PREFIX)) {
                        log.add("appid", data.substring(APPID_PREFIX_LENGTH,
                                                        Math.min(APPID_PREFIX_LENGTH + APPID_MSG_LENGTH,
                                                                 data.length())));
                    }
                }

                log.add("response", getStatusCode(response));
                if (response instanceof ErrorResponse) {
                    log.add("error.code", ((ErrorResponse) response).getErrorNumber());
                    log.add("error.msg", ((ErrorResponse) response).getErrorMessage());
                }

                log.toLogger(logger);
            }
        }

        super.writeRequested(ctx, e);
    }

    private static String getStatusCode(AbstractResponseMessage response)
    {
        short status = response.getBuffer().getShort(2);
        switch (status) {
        case XrootdProtocol.kXR_authmore:
            return "authmore";
        case XrootdProtocol.kXR_error:
            return "error";
        case XrootdProtocol.kXR_ok:
            return "ok";
        case XrootdProtocol.kXR_oksofar:
            return "oksofar";
        case XrootdProtocol.kXR_redirect:
            return "redirect";
        case XrootdProtocol.kXR_wait:
            return "wait";
        case XrootdProtocol.kXR_waitresp:
            return "waitresp";
        default:
            return String.valueOf(status);
        }
    }

    private static String getRequestId(XrootdRequest request)
    {
        switch (request.getRequestId()) {
        case kXR_auth:
            return "auth";
        case kXR_login:
            return "login";
        case kXR_open:
            return "open";
        case kXR_stat:
            return "stat";
        case kXR_statx:
            return "statx";
        case kXR_read:
            return "read";
        case kXR_readv:
            return "readv";
        case kXR_write:
            return "write";
        case kXR_sync:
            return "sync";
        case kXR_close:
            return "close";
        case kXR_protocol:
            return "protocol";
        case kXR_rm:
            return "rm";
        case kXR_rmdir:
            return "rmdir";
        case kXR_mkdir:
            return "mkdir";
        case kXR_mv:
            return "mv";
        case kXR_dirlist:
            return "dirlist";
        case kXR_prepare:
            return "prepare";
        case kXR_locate :
            return "locate";
        case kXR_query :
            return "query";
        case kXR_set :
            return "set";
        default:
            return String.valueOf(request.getRequestId());
        }
    }

    private static CharSequence getUser(Subject subject)
    {
        Long uid = null;
        Long gid = null;
        boolean hasSecondaryGid = false;
        for (Principal principal : subject.getPrincipals()) {
            if (principal instanceof UidPrincipal) {
                if (((UidPrincipal) principal).getUid() == 0) {
                    return "root";
                }
                uid = ((UidPrincipal) principal).getUid();
            } else if (principal instanceof GidPrincipal) {
                if (((GidPrincipal) principal).isPrimaryGroup()) {
                    gid = ((GidPrincipal) principal).getGid();
                } else {
                    hasSecondaryGid = true;
                }
            }
        }
        if (uid == null) {
            return "nobody";
        }
        StringBuilder s = new StringBuilder();
        s.append(uid).append(':');
        if (gid != null) {
            s.append(gid).append(',');
        }
        if (hasSecondaryGid) {
            for (Principal principal : subject.getPrincipals()) {
                if (principal instanceof GidPrincipal) {
                    if (!((GidPrincipal) principal).isPrimaryGroup()) {
                        s.append(((GidPrincipal) principal).getGid()).append(',');
                    }
                }
            }
        }
        return s.subSequence(0, s.length() - 1);
    }

    private static String getAddress(InetSocketAddress addr)
    {
        return addr.getAddress().getHostAddress() + ":" + addr.getPort();
    }
}
