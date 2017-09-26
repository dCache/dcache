/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 Deutsches Elektronen-Synchrotron
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
package org.dcache.macaroons;

import com.github.nitram509.jmacaroons.GeneralCaveatVerifier;
import com.google.common.base.Splitter;

import java.net.InetAddress;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.dcache.util.Subnet;

import static org.dcache.macaroons.InvalidCaveatException.checkCaveat;

/**
 * A CaveatVerifier that checks all supplied 'ip:' caveats are satisfied by the
 * current client.
 */
public class ClientIPCaveatVerifier implements GeneralCaveatVerifier
{
    private final InetAddress address;

    private String error;

    public ClientIPCaveatVerifier(InetAddress address)
    {
        this.address = address;
    }

    @Override
    public boolean verifyCaveat(String serialised)
    {
        try {
            Caveat caveat = new Caveat(serialised);
            if (caveat.getType() == CaveatType.IP) {
                checkAddress(caveat);
                return true;
            }
        } catch (InvalidCaveatException e) {
            error = e.getMessage();
        }
        return false;
    }

    private void checkAddress(Caveat caveat) throws InvalidCaveatException
    {
        checkCaveat(address != null, "client has unknown address");

        List<String> subnets = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(caveat.getValue());
        List<String> badSubnets = subnets.stream()
                .filter(s -> !Subnet.isValid(s))
                .collect(Collectors.toList());
        checkCaveat(badSubnets.isEmpty(), "Invalid subnets: %s", badSubnets);
        checkCaveat(subnets.stream().map(Subnet::create).anyMatch(s -> s.contains(address)),
                "Client fails to match IP caveat %s", caveat);
    }

    public Optional<String> getError()
    {
        return Optional.ofNullable(error);
    }
}
