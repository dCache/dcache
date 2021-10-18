/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
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
package diskCacheV111.vehicles;

import dmg.cells.nucleus.CellAddressCore;
import org.dcache.auth.Subjects;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DoorRequestInfoMessageTest {
    @Test
    public void shouldReturnGidOfSubject() {
        var message = new DoorRequestInfoMessage(new CellAddressCore("myDoor@myDomain"));
        message.setSubject(Subjects.of().username("paul").uid(100).gid(200).build());

        int gid = message.getGid();

        assertThat(gid, is(equalTo(200)));
    }

    @Test
    public void shouldReturnUidOfSubject() {
        var message = new DoorRequestInfoMessage(new CellAddressCore("myDoor@myDomain"));
        message.setSubject(Subjects.of().username("paul").uid(100).gid(200).build());

        int uid = message.getUid();

        assertThat(uid, is(equalTo(100)));
    }

    @Test
    public void shouldReturnDnAsOwnerWithDnInSubject() {
        var message = new DoorRequestInfoMessage(new CellAddressCore("myDoor@myDomain"));
        message.setSubject(Subjects.of().username("paul").uid(100).gid(200)
                .dn("/C=DE/O=GermanGrid/OU=DESY/CN=Alexander Paul Millar").build());

        String owner = message.getOwner();

        assertThat(owner, is(equalTo("/C=DE/O=GermanGrid/OU=DESY/CN=Alexander Paul Millar")));
    }

    @Test
    public void shouldReturnUsernameAsOwnerWithoutDnInSubject() {
        var message = new DoorRequestInfoMessage(new CellAddressCore("myDoor@myDomain"));
        message.setSubject(Subjects.of().username("paul").uid(100).gid(200).build());

        String owner = message.getOwner();

        assertThat(owner, is(equalTo("paul")));
    }

    @Test
    public void shouldReturnDefaultValueForGidOfMissingSubject() {
        var message = new DoorRequestInfoMessage(new CellAddressCore("myDoor@myDomain"));

        int gid = message.getGid();

        assertThat(gid, is(equalTo(-1)));
    }

    @Test
    public void shouldReturnDefaultValueForUidOfMissingSubject() {
        var message = new DoorRequestInfoMessage(new CellAddressCore("myDoor@myDomain"));

        int uid = message.getUid();

        assertThat(uid, is(equalTo(-1)));
    }
}