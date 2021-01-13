package org.dcache.pinmanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import diskCacheV111.util.PnfsId;

import org.dcache.auth.Subjects;
import org.dcache.pinmanager.model.Pin;
import org.dcache.pinmanager.model.Pin.State;
import org.dcache.util.NDC;
import org.dcache.util.TimeUtils;
import org.dcache.util.TimeUtils.TimeUnitFormat;

/**
 * An implementation of PinDao that logs interactions between pin-manager and
 * the persistent storage.
 */
public class LoggingDao implements PinDao
{
    private static final Logger LOG = LoggerFactory.getLogger(LoggingDao.class);

    /** Base class for wrapping either PinCriterion or PinUpdate. */
    private abstract static class LoggingWrappingBuilder<I,W extends I>
    {
        final StringBuilder description;
        final I inner;
        final BiFunction<I,StringBuilder,W> newWrappingBuilder;

        public LoggingWrappingBuilder(I inner, BiFunction<I,StringBuilder,W> newBuilder)
        {
            description = new StringBuilder();
            this.inner = inner;
            this.newWrappingBuilder = newBuilder;
        }

        public LoggingWrappingBuilder(I inner, BiFunction<I,StringBuilder,W> newBuilder, StringBuilder existing)
        {
            description = new StringBuilder(existing);
            this.inner = inner;
            this.newWrappingBuilder = newBuilder;
        }

        protected StringBuilder description()
        {
            if (description.length() == 0) {
                description.append(' ');
            } else {
                description.append(", ");
            }
            return description;
        }

        protected W wrap(I newInnerBuilder)
        {
            return newInnerBuilder == inner
                    ? (W)this
                    : newWrappingBuilder.apply(newInnerBuilder, description);
        }

        public CharSequence getDescription(boolean isUnique)
        {
            if (isUnique) {
                return description;
            } else {
                return description.toString()
                        .replace("has ", "have ")
                        .replace("is ", "are ")
                        .replace("expires", "expire");
            }
        }
    }

    /** Record pin selection criterion. */
    private static class LoggingPinCriterion
            extends LoggingWrappingBuilder<PinCriterion, LoggingPinCriterion>
            implements UniquePinCriterion
    {
        private UniquePinCriterion innerUnique;
        private boolean hasId;
        private boolean hasPnfsId;
        private boolean hasRequestId;

        public boolean isUnique()
        {
            return hasId || hasPnfsId && hasRequestId;
        }

        public String getTarget()
        {
            return isUnique() ? "the pin" : "all pins";
        }

        public String getSubjectPronoun()
        {
            return isUnique() ? "it" : "they";
        }

        public LoggingPinCriterion(PinCriterion inner)
        {
            super(inner, LoggingPinCriterion::new);
            updateUniquePin(inner);
        }

        public LoggingPinCriterion(PinCriterion inner, StringBuilder existing)
        {
            super(inner, LoggingPinCriterion::new, existing);
            updateUniquePin(inner);
        }

        private void updateUniquePin(PinCriterion inner)
        {
            if (inner instanceof UniquePinCriterion) {
                innerUnique = (UniquePinCriterion)inner;
            }
        }

        @Override
        protected LoggingPinCriterion wrap(PinCriterion newInnerBuilder)
        {
            updateUniquePin(inner);
            return super.wrap(newInnerBuilder);
        }

        @Override
        public UniquePinCriterion id(long id)
        {
            hasId = true;
            description().append("with id ").append(id);
            return wrap(inner.id(id));
        }

        @Override
        public UniquePinCriterion pnfsId(PnfsId id)
        {
            hasPnfsId = true;
            description().append("with PNFS-ID ").append(id);
            return wrap(inner.pnfsId(id));
        }

        @Override
        public UniquePinCriterion requestId(String requestId)
        {
            hasRequestId = true;
            description().append("with request ID ").append(requestId);
            return wrap(inner.requestId(requestId));
        }

        @Override
        public UniquePinCriterion expirationTimeBefore(Date date)
        {
            StringBuilder sb = description().append("that expires before ");
            TimeUtils.appendRelativeTimestamp(sb, date.getTime(),
                    System.currentTimeMillis(), TimeUnitFormat.SHORT);

            return wrap(inner.expirationTimeBefore(date));
        }

        @Override
        public UniquePinCriterion state(State state)
        {
            description().append("with state ").append(state);
            return wrap(inner.state(state));
        }

        @Override
        public UniquePinCriterion stateIsNot(State state)
        {
            description().append("not with state ").append(state);
            return wrap(inner.stateIsNot(state));
        }

        @Override
        public UniquePinCriterion pool(String pool)
        {
            description().append("on pool ").append(pool);
            return wrap(inner.pool(pool));
        }

        @Override
        public UniquePinCriterion sticky(String sticky)
        {
            description().append("with sticky ").append(sticky);
            return wrap(inner.sticky(sticky));
        }

        @Override
        public UniquePinCriterion sameIdAs(UniquePinCriterion c)
        {
            return wrap(inner.sameIdAs(c));
        }

        public CharSequence getDescription()
        {
            if (isUnique()) {
                return description;
            } else {
                return description.toString()
                        .replace("expires ", "expire ");
            }
        }
    }

    /** Record how pin is to be updated or created. */
    private static class LoggingPinUpdate
            extends LoggingWrappingBuilder<PinUpdate, LoggingPinUpdate>
            implements PinUpdate
    {
        public LoggingPinUpdate(PinUpdate inner)
        {
            super(inner, LoggingPinUpdate::new);
        }

        public LoggingPinUpdate(PinUpdate inner, StringBuilder existing)
        {
            super(inner, LoggingPinUpdate::new, existing);
        }


        @Override
        public PinUpdate expirationTime(Date expirationTime)
        {
            if (expirationTime != null) {
                StringBuilder sb = description().append("expires at ");
                TimeUtils.appendRelativeTimestamp(sb, expirationTime.getTime(),
                        System.currentTimeMillis(), TimeUnitFormat.SHORT);
            } else {
                description().append("never expires");
            }
            return wrap(inner.expirationTime(expirationTime));
        }

        @Override
        public PinUpdate pool(String pool)
        {
            description().append("is located on ").append(pool);
            return wrap(inner.pool(pool));
        }

        @Override
        public PinUpdate requestId(String requestId)
        {
            description().append("has request ID ").append(requestId);
            return wrap(inner.requestId(requestId));
        }

        @Override
        public PinUpdate state(State state)
        {
            description().append("has state ").append(state);
            return wrap(inner.state(state));
        }

        @Override
        public PinUpdate sticky(String sticky)
        {
            description().append("has sticky ").append(sticky);
            return wrap(inner.sticky(sticky));
        }

        @Override
        public PinUpdate subject(Subject subject)
        {
            // REVISIT: here we assume what information the inner DAO records.
            description().append("has owner uid=").append(Subjects.getUid(subject))
                    .append(" and gid=").append(Subjects.getPrimaryGid(subject));
            return wrap(inner.subject(subject));
        }

        @Override
        public PinUpdate pnfsId(PnfsId pnfsId)
        {
            description().append("has PNFS-ID ").append(pnfsId);
            return wrap(inner.pnfsId(pnfsId));
        }
    }

    private final PinDao inner;
    private final AtomicInteger foreachCounter = new AtomicInteger();

    public LoggingDao(PinDao inner)
    {
        this.inner = inner;
    }

    @Override
    public PinCriterion where()
    {
        return new LoggingPinCriterion(inner.where());
    }

    @Override
    public PinUpdate set()
    {
        return new LoggingPinUpdate(inner.set());
    }

    @Override
    public Pin create(PinUpdate update)
    {
        LoggingPinUpdate u = (LoggingPinUpdate)update;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating pin in database that{}.", u.getDescription(true));
        }
        return inner.create(u.inner);
    }

    @Override
    public List<Pin> get(PinCriterion criterion)
    {
        LoggingPinCriterion c = (LoggingPinCriterion) criterion;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Obtaining information about {}{}.", c.getTarget(),
                    c.getDescription());
        }
        return inner.get(c.inner);
    }

    @Override
    public List<Pin> get(PinCriterion criterion, int limit)
    {
        LoggingPinCriterion c = (LoggingPinCriterion) criterion;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Obtaining information about at most {} pins{}.", limit,
                    c.getDescription());
        }
        return inner.get(c.inner, limit);
    }

    @Override
    public Pin get(UniquePinCriterion criterion)
    {
        LoggingPinCriterion c = (LoggingPinCriterion) criterion;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Obtaining information about {}{}.", c.getTarget(),
                    c.getDescription());
        }
        return inner.get(c.innerUnique);
    }

    @Override
    public int count(PinCriterion criterion)
    {
        LoggingPinCriterion c = (LoggingPinCriterion) criterion;
        if (LOG.isDebugEnabled()) {
            String operation = c.isUnique() ? "Checking existance of" : "Counting";
            LOG.debug("{} {}{}.", operation, c.getTarget(), c.getDescription());
        }

        return inner.count(c.inner);
    }

    @Override
    public Pin update(UniquePinCriterion criterion, PinUpdate update)
    {
        LoggingPinCriterion c = (LoggingPinCriterion) criterion;
        LoggingPinUpdate u = (LoggingPinUpdate) update;
        logUpdate(c, u);
        return inner.update(c.innerUnique, u.inner);
    }

    @Override
    public int update(PinCriterion criterion, PinUpdate update)
    {
        LoggingPinCriterion c = (LoggingPinCriterion) criterion;
        LoggingPinUpdate u = (LoggingPinUpdate) update;
        logUpdate(c, u);
        return inner.update(c.inner, u.inner);
    }

    private void logUpdate(LoggingPinCriterion criterion, LoggingPinUpdate update)
    {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Updating {}{} so {}{}.", criterion.getTarget(),
                    criterion.getDescription(), criterion.getSubjectPronoun(),
                    update.getDescription(criterion.isUnique()));
        }
    }

    @Override
    public int delete(PinCriterion criterion)
    {
        LoggingPinCriterion c = (LoggingPinCriterion) criterion;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting {}{}.", c.getTarget(), c.getDescription());
        }
        return inner.delete(c.inner);
    }

    @Override
    public void foreach(PinCriterion criterion, InterruptibleConsumer<Pin> f)
            throws InterruptedException
    {
        LoggingPinCriterion c = (LoggingPinCriterion) criterion;

        if (LOG.isDebugEnabled()) {
            String id = "FOREACH-" + foreachCounter.incrementAndGet();
            LOG.debug("Operating on {}{} as {}.", c.getTarget(),
                    c.getDescription(), id);
            NDC.push(id);
        }

        try {
            inner.foreach(c.inner, f);
        } finally {
            if (LOG.isDebugEnabled()) {
                NDC.pop();
            }
        }    }

    @Override
    public void foreach(PinCriterion criterion, InterruptibleConsumer<Pin> f, int limit)
            throws InterruptedException
    {
        LoggingPinCriterion c = (LoggingPinCriterion) criterion;

        if (LOG.isDebugEnabled()) {
            String limitString = " (limit: " + limit + ")";
            String id = "FOREACH-" + foreachCounter.incrementAndGet();
            LOG.debug("Operating on {}{} as {}{}.", c.getTarget(),
                    c.getDescription(), id, limitString);
            NDC.push(id);
        }

        try {
            inner.foreach(c.inner, f, limit);
        } finally {
            if (LOG.isDebugEnabled()) {
                NDC.pop();
            }
        }
    }
}
