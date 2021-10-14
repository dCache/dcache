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
package diskCacheV111.srm.dcache;

import static org.dcache.util.ByteUnit.KiB;

import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.Striped;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import org.springframework.dao.IncorrectResultSizeDataAccessException;

/**
 * A store of byte arrays.
 * <p>
 * Each stored byte array gets an ID based on its hash with rehashing to resolve collisions. Storing
 * the same byte array twice may reuse the generated ID, but this is not guaranteed.
 * <p>
 * Actual storage of byte arrays is not implemented by this class. Create, read and delete
 * operations are delegated to functions injected through the constructor.
 * <p>
 * Stored byte arrays are referenced in memory by instances of the {@code Token} class. As long as a
 * byte array is referenced by a Token instance, the byte array is kept in the store. Once no longer
 * referenced, a byte array becomes eligible for garbage collection.
 * <p>
 * Unreferenced entries may be garbage collected which can both lead to unused IDs being reused as
 * well as new byte arrays getting an ID that differs from those assigned to identical byte arrays
 * stored previously.
 * <p>
 * The class implements a cache to reduce the frequency with which the create, read and delete
 * functions are called.
 */
@ParametersAreNonnullByDefault
public final class CanonicalizingByteArrayStore {

    /* Arbitrary value for the first half of the hash key. */
    private final long K0 = 0x0706050403020100L;

    /**
     * A Token is a reference to a stored byte array. As long as a hard reference is maintained to
     * the token, the byte array cannot be garbage collected.
     */
    public static class Token {

        private final long id;

        private Token(long id) {
            this.id = id;
        }

        public long getId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Token token = (Token) o;
            return id == token.id;
        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }
    }

    private final BiConsumer<Long, byte[]> create;
    private final Function<Long, byte[]> read;
    private final Consumer<Long> delete;

    /**
     * Cache to reduce frequent reloads from the database.
     */
    private final Cache<Long, byte[]> cache =
          CacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).maximumSize(1000)
                .build();

    /**
     * Cache to canonicalise Token instances and track in memory references. This is to prevent
     * garbage collecting users in the database when they are still referenced in memory.
     * <p>
     * Class invariant: Any Token has a corresponding byte array in the database.
     */
    private final Cache<Long, Token> canonicalizationCache = CacheBuilder.newBuilder().weakValues()
          .build();

    /**
     * Operations are synchronized on byte array IDs.
     * <p>
     * Class invariant: Entries will no be added to {@code cache} or {@code canonicalizationCache},
     * tokens will not be generated, nor will the database be modified without locking the
     * corresponding ID.
     */
    private final Striped<Lock> locks = Striped.lazyWeakLock(KiB.toBytes(4));

    public CanonicalizingByteArrayStore(
          BiConsumer<Long, byte[]> create, Function<Long, byte[]> read, Consumer<Long> delete) {
        this.create = create;
        this.read = read;
        this.delete = delete;
    }

    /**
     * Returns a {@code Token} for a particular byte array ID.
     * <p>
     * As long as the token is referenced, the byte array is not eligible for garbage collection.
     *
     * @param id a byte array id
     * @return A {@code Token} representing the byte array corresponding to {@code id} or null if
     * such a byte array is not stored in the database.
     */
    @Nullable
    public Token toToken(long id) {
        Lock lock = locks.get(id);
        lock.lock();
        try {
            return (load(id) != null) ? makeToken(id) : null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns a {@code Token} for a particular byte array.
     * <p>
     * As long as the token is referenced, the byte array is not eligible for garbage collection.
     * <p>
     * The byte array ID can be extracted from the token.
     *
     * @param bytes a byte array
     * @return A {@code Token} referencing the given byte array
     */
    @Nonnull
    public Token toToken(byte[] bytes) {
        Token token = null;
        long k1 = 0x00;
        do {
            HashCode hash = Hashing.sipHash24(K0, k1++).hashBytes(bytes);
            long id = hash.asLong();
            Lock lock = locks.get(id);
            lock.lock();
            try {
                byte[] canonical = load(id);
                if (canonical == null) {
                    save(id, bytes);
                    token = makeToken(id);
                } else if (Arrays.equals(bytes, canonical)) {
                    token = makeToken(id);
                }
            } finally {
                lock.unlock();
            }
        } while (token == null);
        return token;
    }

    /**
     * Returns the byte array referenced by a {@code Token}.
     *
     * @param token A byte array {@code Token}.
     * @return The byte array corresponding to the token.
     */
    @Nonnull
    public byte[] readBytes(Token token) {
        long id = token.getId();
        Lock lock = locks.get(id);
        lock.lock();
        try {
            byte[] bytes = load(id);
            if (bytes == null) {
                throw new IncorrectResultSizeDataAccessException(1, 0);
            }
            return bytes;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns or generates a canonical token for the given id.
     */
    private Token makeToken(long id) {
        try {
            return canonicalizationCache.get(id, () -> new Token(id));
        } catch (UncheckedExecutionException | ExecutionException e) {
            Throwable cause = e.getCause();
            Throwables.throwIfUnchecked(cause);
            throw new RuntimeException(cause);
        }
    }

    private void save(long id, byte[] bytes) {
        create.accept(id, bytes);
        cache.put(id, bytes);
    }

    private byte[] load(long id) {
        byte[] bytes = cache.getIfPresent(id);
        if (bytes == null) {
            bytes = read.apply(id);
            if (bytes != null) {
                cache.put(id, bytes);
            }
        }
        return bytes;
    }

    /**
     * Garbage collects unreferenced byte arrays.
     * <p>
     * The byte arrays of the given IDs are removed from the database if no longer referenced by any
     * {@code Token} instances.
     *
     * @param ids The IDs to garbage collect.
     */
    public void gc(List<Long> ids) {
        canonicalizationCache.cleanUp();

        for (Long id : ids) {
            Lock lock = locks.get(id);
            lock.lock();
            try {
                if (canonicalizationCache.getIfPresent(id) == null) {
                    cache.invalidate(id);
                    delete.accept(id);
                }
            } finally {
                lock.unlock();
            }
        }
    }
}
