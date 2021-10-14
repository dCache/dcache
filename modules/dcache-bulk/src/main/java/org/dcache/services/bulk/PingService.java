package org.dcache.services.bulk;

import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessageReceiver;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.dcache.cells.MessageReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

public class PingService implements CellLifeCycleAware,
      CellMessageReceiver,
      Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PingService.class);
    private static final Random RANDOM = new Random();

    private ScheduledExecutorService executorService;
    private int maxWait = 1;
    private TimeUnit maxWaitUnit = TimeUnit.MINUTES;
    private long max = maxWaitUnit.toMillis(maxWait);

    private AtomicLong received = new AtomicLong(0);
    private AtomicLong replied = new AtomicLong(0);

    private class PingReply implements Comparable<PingReply> {

        final String id;
        final long expires;
        final PingMessage message;
        final MessageReply<PingMessage> messageReply;

        PingReply(PingMessage message) {
            this.message = message;
            this.messageReply = new MessageReply<>();
            expires = System.currentTimeMillis() + getRandomMillis();
            id = UUID.randomUUID().toString();
        }

        @Override
        public int compareTo(PingReply other) {
            int compared = Long.compare(expires, other.expires);
            if (compared == 0) {
                compared = id.compareTo(other.id);
            }

            return compared;
        }
    }

    private final TreeSet<PingReply> replies = new TreeSet<>();

    @Override
    public void afterStart() {
        /*
         *  reset
         */
        max = maxWaitUnit.toMillis(maxWait);
        run();
    }

    public MessageReply<PingMessage> messageArrived(PingMessage message) {
        /*
         *  Work is minimal, do it on message thread.
         */
        synchronized (replies) {
            PingReply pingReply = new PingReply(message);
            replies.add(pingReply);
            received.incrementAndGet();
            LOGGER.info("[RECEIVED] {}, expires {}, total received {}, replies {}.",
                  message.getKey(),
                  new Date(pingReply.expires),
                  received.get(),
                  replies.size());
            return pingReply.messageReply;
        }
    }

    public void run() {
        long delay = TimeUnit.SECONDS.toMillis(5);
        long now = System.currentTimeMillis();

        synchronized (replies) {
            LOGGER.info("BEGIN REPLY waiting {}, total replies {}.",
                  replies.size(), replied.get());
            if (!replies.isEmpty()) {
                for (Iterator<PingReply> it = replies.iterator(); it.hasNext(); ) {
                    PingReply pingReply = it.next();
                    if (pingReply.expires <= now) {
                        pingReply.messageReply.reply(pingReply.message);
                        it.remove();
                        LOGGER.info("[REPLIED] to {}.", pingReply.message.getKey());
                        replied.incrementAndGet();
                    } else {
                        delay = pingReply.expires - now;
                        break;
                    }
                }
                LOGGER.info("END REPLY waiting {}, total replies {}.",
                      replies.size(), replied.get());
            }
        }

        executorService.schedule(this, delay, TimeUnit.MILLISECONDS);
    }

    @Required
    public void setExecutorService(ScheduledExecutorService executorService) {
        this.executorService = executorService;
    }

    @Required
    public void setMaxWait(int maxWait) {
        this.maxWait = maxWait;
    }

    @Required
    public void setMaxWaitUnit(TimeUnit maxWaitUnit) {
        this.maxWaitUnit = maxWaitUnit;
    }

    private long getRandomMillis() {
        return Math.abs(RANDOM.nextLong() % max);
    }
}
