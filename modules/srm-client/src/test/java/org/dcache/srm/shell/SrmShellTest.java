/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 - 2020 Deutsches Elektronen-Synchrotron
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
package org.dcache.srm.shell;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.regex.Matcher;
import org.junit.Test;

public class SrmShellTest {

    @Test
    public void shouldMatchSimpleDN() {
        Matcher m = SrmShell.DN_WITH_CAPTURED_CN.matcher(
              "/C=UK/O=eScience/OU=Glasgow/L=Compserv/CN=graeme stewart");

        assertThat(m.matches(), is(equalTo(true)));
        assertThat(m.group("cn"), is(equalTo("graeme stewart")));
    }

    @Test
    public void shouldMatchMoreAwkwardDN() {
        Matcher m = SrmShell.DN_WITH_CAPTURED_CN.matcher(
              "/C=IT/O=INFN/OU=Personal Certificate/L=Pisa/CN=Flavia Donno/Email=flavia.donno@pi.infn.it");

        assertThat(m.matches(), is(equalTo(true)));
        assertThat(m.group("cn"), is(equalTo("Flavia Donno")));
    }
}
