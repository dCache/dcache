/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
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
package org.dcache.services.ssh2;

import org.apache.sshd.server.Command;
import org.apache.sshd.server.Environment;
import org.apache.sshd.server.ExitCallback;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import diskCacheV111.admin.UserAdminShell;

public class ShellCommand implements Command
{
    private final File historyFile;
    private final int historySize;
    private final boolean useColor;
    private final UserAdminShell shell;
    private InputStream in;
    private OutputStream out;
    private OutputStream err;
    private ExitCallback callback;

    private Command delegate;

    public ShellCommand(File historyFile, int historySize, boolean useColor, UserAdminShell shell)
    {
        this.historyFile = historyFile;
        this.historySize = historySize;
        this.useColor = useColor;
        this.shell = shell;
    }

    @Override
    public void setInputStream(InputStream in)
    {
        this.in = in;
    }

    @Override
    public void setOutputStream(OutputStream out)
    {
        this.out = out;
    }

    @Override
    public void setErrorStream(OutputStream err)
    {
        this.err = err;
    }

    @Override
    public void setExitCallback(ExitCallback callback)
    {
        this.callback = callback;
    }

    @Override
    public void start(Environment env) throws IOException
    {
        if (env.getEnv().get(Environment.ENV_TERM) != null) {
            delegate = new AnsiTerminalCommand(historyFile, historySize, useColor, shell);
        } else {
            delegate = new NoTerminalCommand(shell);
        }
        delegate.setInputStream(in);
        delegate.setOutputStream(out);
        delegate.setErrorStream(err);
        delegate.setExitCallback(callback);
        delegate.start(env);
    }

    @Override
    public void destroy() throws Exception
    {
        if (delegate != null) {
            delegate.destroy();
        }
    }
}
