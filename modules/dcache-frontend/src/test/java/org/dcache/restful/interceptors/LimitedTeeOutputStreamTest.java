/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2020 Deutsches Elektronen-Synchrotron
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
package org.dcache.restful.interceptors;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;

public class LimitedTeeOutputStreamTest
{
    private ByteArrayOutputStream out;
    private ByteArrayOutputStream branch;
    private LimitedTeeOutputStream tee;

    @Before
    public void setup()
    {
        out = new ByteArrayOutputStream();
        branch = new ByteArrayOutputStream();
    }

    @Test
    public void shouldBeInitiallyEmpty()
    {
        given(limitedTee().withLimit(10));

        assertThat(out.size(), equalTo(0));
        assertThat(branch.size(), equalTo(0));
        assertFalse(tee.isBranchTruncated());
    }

    @Test
    public void shouldTeeAllSubLimitInputWithByteArray() throws Exception
    {
        byte[] testData = "Test data".getBytes(UTF_8);
        assert testData.length == 9;

        given(limitedTee().withLimit(10));

        tee.write(testData);

        assertThat(out.toByteArray(), equalTo(testData));
        assertThat(branch.toByteArray(), equalTo(testData));
        assertFalse(tee.isBranchTruncated());
    }

    @Test
    public void shouldTeeAllSubLimitInputWithByte() throws Exception
    {
        byte[] testData = "Test data".getBytes(UTF_8);
        assert testData.length == 9;

        given(limitedTee().withLimit(10));

        for (byte b : testData) {
            tee.write(b);
        }

        assertThat(out.toByteArray(), equalTo(testData));
        assertThat(branch.toByteArray(), equalTo(testData));
        assertFalse(tee.isBranchTruncated());
    }

    @Test
    public void shouldTeeAllSubLimitInputWithByteArrayRange() throws Exception
    {
        byte[] testData = "Test data".getBytes(UTF_8);
        assert testData.length == 9;

        given(limitedTee().withLimit(10));

        tee.write(testData, 0, testData.length);

        assertThat(out.toByteArray(), equalTo(testData));
        assertThat(branch.toByteArray(), equalTo(testData));
        assertFalse(tee.isBranchTruncated());
    }

    @Test
    public void shouldTeeAllLimitInputWithByteArray() throws Exception
    {
        byte[] testData = "Test data!".getBytes(UTF_8);
        assert testData.length == 10;

        given(limitedTee().withLimit(10));

        tee.write(testData);

        assertThat(out.toByteArray(), equalTo(testData));
        assertThat(branch.toByteArray(), equalTo(testData));
        assertFalse(tee.isBranchTruncated());
    }

    @Test
    public void shouldTeeAllLimitInputWithByte() throws Exception
    {
        byte[] testData = "Test data!".getBytes(UTF_8);
        assert testData.length == 10;

        given(limitedTee().withLimit(10));

        for (byte b : testData) {
            tee.write(b);
        }

        assertThat(out.toByteArray(), equalTo(testData));
        assertThat(branch.toByteArray(), equalTo(testData));
        assertFalse(tee.isBranchTruncated());
    }

    @Test
    public void shouldTeeAllLimitInputWithByteArrayRange() throws Exception
    {
        byte[] testData = "Test data!".getBytes(UTF_8);
        assert testData.length == 10;

        given(limitedTee().withLimit(10));

        tee.write(testData, 0, testData.length);

        assertThat(out.toByteArray(), equalTo(testData));
        assertThat(branch.toByteArray(), equalTo(testData));
        assertFalse(tee.isBranchTruncated());
    }

    @Test
    public void shouldTruncateBranchWithLargeByteArrayInput() throws Exception
    {
        byte[] testData = "My test data!".getBytes(UTF_8);
        assert testData.length == 13;

        given(limitedTee().withLimit(10));

        tee.write(testData);

        assertThat(out.toByteArray(), equalTo(testData));
        assertThat(branch.toByteArray(), equalTo("My test da".getBytes(UTF_8)));
        assertTrue(tee.isBranchTruncated());
    }

    @Test
    public void shouldTruncateBranchWithLargeSingleByteInput() throws Exception
    {
        byte[] testData = "My test data!".getBytes(UTF_8);
        assert testData.length == 13;

        given(limitedTee().withLimit(10));

        for (byte b : testData) {
            tee.write(b);
        }

        assertThat(out.toByteArray(), equalTo(testData));
        assertThat(branch.toByteArray(), equalTo("My test da".getBytes(UTF_8)));
        assertTrue(tee.isBranchTruncated());
    }

    @Test
    public void shouldTruncateBranchWithLargeSingleByteArrayOffsetInput() throws Exception
    {
        byte[] testData = "My test data!".getBytes(UTF_8);
        assert testData.length == 13;

        given(limitedTee().withLimit(10));

        tee.write(testData, 0, testData.length);

        assertThat(out.toByteArray(), equalTo(testData));
        assertThat(branch.toByteArray(), equalTo("My test da".getBytes(UTF_8)));
        assertTrue(tee.isBranchTruncated());
    }

    private void given(LimitedTeeBuilder builder)
    {
        tee = builder.build();
    }

    private LimitedTeeBuilder limitedTee()
    {
        return new LimitedTeeBuilder();
    }

    private class LimitedTeeBuilder
    {
        private long limit;

        LimitedTeeBuilder withLimit(long value)
        {
            limit = value;
            return this;
        }

        LimitedTeeOutputStream build()
        {
            return new LimitedTeeOutputStream(out, branch, limit);
        }
    }
}
