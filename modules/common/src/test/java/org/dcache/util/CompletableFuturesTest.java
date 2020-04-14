package org.dcache.util;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CompletableFuturesTest {

    @Test
    public void shouldNotBeDoneIfCompletableNotDone() {

        CompletableFuture<Void> completable = new CompletableFuture<>();

        ListenableFuture<Void> listenable = CompletableFutures.fromCompletableFuture(completable);
        assertFalse(listenable.isDone());
    }

    @Test
    public void shouldNotBeDoneIfListenableFutureNotDone() {

        ListenableFuture<Void> listenable = SettableFuture.create();
        CompletableFuture<Void> completable = CompletableFutures.fromListenableFuture(listenable);

        assertFalse(completable.isDone());
    }

    @Test
    public void shouldBeDoneWhenCompletableIsDone() {

        CompletableFuture<Void> completable = CompletableFuture.completedFuture(null);

        ListenableFuture<Void> listenable = CompletableFutures.fromCompletableFuture(completable);
        assertTrue(listenable.isDone());
    }

    @Test
    public void shouldBeDoneWhenListenableFutureIsDone() {

        ListenableFuture<Void> listenable = Futures.immediateFuture(null);
        CompletableFuture<Void> completable = CompletableFutures.fromListenableFuture(listenable);

        assertTrue(completable.isDone());
    }

    @Test
    public void shouldCompleteWhenCompletableIsDone() {

        CompletableFuture<Void> completable = new CompletableFuture<>();

        ListenableFuture<Void> listenable = CompletableFutures.fromCompletableFuture(completable);
        completable.complete(null);
        assertTrue(listenable.isDone());
    }

    @Test
    public void shouldCompleteWhenListenableIsDone() {

        SettableFuture<Void> listenable = SettableFuture.create();
        CompletableFuture<Void> completable = CompletableFutures.fromListenableFuture(listenable);

        listenable.set(null);
        assertTrue(completable.isDone());
    }

    @Test(expected = ExecutionException.class)
    public void shouldFailWhenCompletableCreatedFailed() throws InterruptedException, ExecutionException {

        CompletableFuture<Void> completable = CompletableFuture.failedFuture(new IOException());

        ListenableFuture<Void> listenable = CompletableFutures.fromCompletableFuture(completable);
        assertTrue(listenable.isDone());
        listenable.get();
    }

    @Test(expected = ExecutionException.class)
    public void shouldFailWhenCompletableIsFailed() throws InterruptedException, ExecutionException {

        CompletableFuture<Void> completable = new CompletableFuture<>();
        ListenableFuture<Void> listenable = CompletableFutures.fromCompletableFuture(completable);

        completable.completeExceptionally(new IOException());

        assertTrue(listenable.isDone());
        listenable.get();
    }

    @Test(expected = ExecutionException.class)
    public void shouldFailWhenListenableIsFailed() throws InterruptedException, ExecutionException {

        ListenableFuture<Void> listenable = Futures.immediateFailedFuture(new IOException());
        CompletableFuture<Void> completable = CompletableFutures.fromListenableFuture(listenable);

        assertTrue(completable.isDone());
        completable.get();
    }

    @Test(expected = ExecutionException.class)
    public void shouldFailWhenListenableCompletesExceptionally() throws InterruptedException, ExecutionException {

        SettableFuture<Void> listenable = SettableFuture.create();
        CompletableFuture<Void> completable = CompletableFutures.fromListenableFuture(listenable);

        listenable.setException(new IOException());
        assertTrue(completable.isDone());
        completable.get();
    }

    @Test
    public void shouldCancelWhenCompletableIsCanceled() throws InterruptedException, ExecutionException {

        CompletableFuture<Void> completable = new CompletableFuture<>();
        ListenableFuture<Void> listenable = CompletableFutures.fromCompletableFuture(completable);

        completable.cancel(true);
        assertTrue(listenable.isDone());
        assertTrue(listenable.isCancelled());
    }

    @Test
    public void shouldCancelWhenListenableIsCanceled() throws InterruptedException, ExecutionException {

        SettableFuture<Void> listenable = SettableFuture.create();
        CompletableFuture<Void> completable = CompletableFutures.fromListenableFuture(listenable);

        listenable.cancel(true);
        assertTrue(completable.isDone());
        assertTrue(completable.isCancelled());
    }

}
