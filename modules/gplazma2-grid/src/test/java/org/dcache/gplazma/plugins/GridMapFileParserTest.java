/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2022 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.plugins;

import java.util.Arrays;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class GridMapFileParserTest
{
    @Test
    public void shouldMatchSimpleDn() throws Exception {
        var map = parse("\"/C=DE/O=GermanGrid/OU=DESY/CN=Alexander Paul Millar\" paul");

        var results = map.getValuesForPredicatesMatching("/C=DE/O=GermanGrid/OU=DESY/CN=Alexander Paul Millar");

        assertThat(results, contains("paul"));
    }

    @Test
    public void shouldMatchLowerCaseDnCaseInsensitive() throws Exception {
        var map = parse("\"/C=DE/O=CTW/street=Sesame/CN=Big Bird\" bbird");

        var results = map.getValuesForPredicatesMatching("/C=DE/O=CTW/STREET=Sesame/CN=Big Bird");

        assertThat(results, contains("bbird"));
    }

    @Test
    public void shouldMatchUpperCaseDnCaseInsensitive() throws Exception {
        var map = parse("\"/C=DE/O=CTW/STREET=Sesame/CN=Big Bird\" bbird");

        var results = map.getValuesForPredicatesMatching("/C=DE/O=CTW/street=Sesame/CN=Big Bird");

        assertThat(results, contains("bbird"));
    }

    @Test
    public void shouldNotMatchDifferentDn() throws Exception {
        var map = parse("\"/C=DE/O=CTW/STREET=Sesame/CN=Big Bird\" bbird");

        var results = map.getValuesForPredicatesMatching("/C=DE/O=GermanGrid/OU=DESY/CN=Alexander Paul Millar");

        assertThat(results, is(empty()));
    }

    @Test
    public void shouldMatchFirstLine() throws Exception {
        var map = parse("\"/C=DE/O=CTW/STREET=Sesame/CN=Big Bird\" bbird",
                "\"/C=DE/O=GermanGrid/OU=DESY/CN=Alexander Paul Millar\" paul");

        var results = map.getValuesForPredicatesMatching("/C=DE/O=CTW/STREET=Sesame/CN=Big Bird");

        assertThat(results, contains("bbird"));
    }

    @Test
    public void shouldMatchSecondLine() throws Exception {
        var map = parse("\"/C=DE/O=CTW/STREET=Sesame/CN=Big Bird\" bbird",
                "\"/C=DE/O=GermanGrid/OU=DESY/CN=Alexander Paul Millar\" paul");

        var results = map.getValuesForPredicatesMatching("/C=DE/O=GermanGrid/OU=DESY/CN=Alexander Paul Millar");

        assertThat(results, contains("paul"));
    }

    @Test
    public void shouldIgnoreTrailingSpace() throws Exception {
        var map = parse("\"/C=DE/O=GermanGrid/OU=DESY/CN=Alexander Paul Millar\" paul ");

        var results = map.getValuesForPredicatesMatching("/C=DE/O=GermanGrid/OU=DESY/CN=Alexander Paul Millar");

        assertThat(results, contains("paul"));
    }

    private PredicateMap<String,String> parse(String... lines) {
        var parser = new GridMapFileParser();
        Arrays.stream(lines).forEach(parser::accept);
        return parser.build();
    }
}
