package org.dcache.util;

import org.junit.Test;
import org.slf4j.MDC;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class NDCTests
{
    @Test
    public void shouldBeNullInitially()
    {
        givenEmptyNdc();

        // REVISIT: is this correct?
        assertThat(ndc(), is(nullValue()));
    }

    @Test
    public void shouldPopOnEmpty()
    {
        givenEmptyNdc();

        String popped = NDC.pop();

        assertThat(ndc(), is(nullValue()));
        assertThat(popped, is(nullValue()));
    }

    @Test
    public void shouldHaveItemAfterPush()
    {
        givenEmptyNdc();

        NDC.push("item-1");

        assertThat(ndc(), is("item-1"));
    }

    @Test
    public void shouldBeNullAfterPushPop()
    {
        givenNdc().push("item-1");

        String popped = NDC.pop();

        // REVISIT: is this correct?
        assertThat(ndc(), is(nullValue()));
        assertThat(popped, is("item-1"));
    }

    @Test
    public void shouldBeNullAfterPushClear()
    {
        givenNdc().push("item-1");

        NDC.clear();

        // REVISIT: is this correct?
        assertThat(ndc(), is(nullValue()));
    }

    @Test
    public void shouldHaveFirstItemAfterTwoPushThenPop()
    {
        givenNdc().push("item-1").push("item-2");

        String popped = NDC.pop();

        assertThat(ndc(), is("item-1"));
        assertThat(popped, is ("item-2"));
    }

    @Test
    public void shouldBeNullAfterTwoPushesThenTwoPops()
    {
        givenNdc().push("item-1").push("item-2").pop();

        String popped = NDC.pop();

        // REVISIT: is this correct?
        assertThat(ndc(), is(nullValue()));
        assertThat(popped, is("item-1"));
    }



    @Test
    public void shouldBeTwoItemsAfterTwoPushesThenPopThenPush()
    {
        givenNdc().push("item-1").push("item-2").pop();

        NDC.push("item-3");

        assertThat(ndc(), is("item-1 item-3"));
    }

    @Test
    public void shouldRestoreClonedValueAfterSet()
    {
        NDC cloned = givenClonedNdc(givenNdc().push("item-1"));
        givenNdc().push("item-2");

        NDC.set(cloned);

        assertThat(ndc(), is("item-1"));
    }

    private String ndc()
    {
        return MDC.get(NDC.KEY_NDC);
    }

    private NdcBuilder givenNdc()
    {
        return new NdcBuilder();
    }

    private NDC givenClonedNdc(NdcBuilder builder)
    {
        return builder.clone();
    }

    private void givenEmptyNdc()
    {
        NDC.clear();
    }

    private class NdcBuilder
    {
        public NdcBuilder()
        {
            NDC.clear();
        }

        public NdcBuilder push(String value)
        {
            NDC.push(value);
            return this;
        }

        public NdcBuilder pop()
        {
            NDC.pop();
            return this;
        }

        public NDC clone()
        {
            return NDC.cloneNdc();
        }
    }
}
