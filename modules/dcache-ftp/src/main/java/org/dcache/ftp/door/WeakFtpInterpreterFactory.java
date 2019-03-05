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
package org.dcache.ftp.door;


import java.util.Optional;

import diskCacheV111.util.ConfigurationException;
import diskCacheV111.util.FsPath;

import org.dcache.util.Args;
import org.dcache.util.Option;


public class WeakFtpInterpreterFactory extends FtpInterpreterFactory
{
    @Option(name="username-password-enabled", required=true)
    private boolean allowUsernamePassword;

    @Option(name="anonymous-enabled", required=true)
    private boolean anonymousEnabled;

    @Option(name="anonymous-user", required=true)
    private String anonymousUser;

    @Option(name="anonymous-email-required", required=true)
    private boolean requireAnonEmailPassword;

    @Option(name="anonymous-root", required=true)
    private FsPath anonymousRoot;

    private Optional<String> anonUser;

    @Override
    public void configure(Args args) throws ConfigurationException
    {
        super.configure(args);

        anonUser = anonymousEnabled
                ? Optional.of(anonymousUser)
                : Optional.empty();
    }

    @Override
    protected AbstractFtpDoorV1 createInterpreter()
    {
        return new WeakFtpDoorV1(allowUsernamePassword, anonUser, anonymousRoot,
                requireAnonEmailPassword);
    }
}
