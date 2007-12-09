package org.dcache.tests.repository;

import java.util.concurrent.atomic.AtomicInteger;

import diskCacheV111.util.event.CacheEvent;
import diskCacheV111.util.event.CacheNeedSpaceEvent;
import diskCacheV111.util.event.CacheRepositoryEvent;
import diskCacheV111.util.event.CacheRepositoryListener;

public class RepositoryCallbacksHelper implements CacheRepositoryListener {

    private final AtomicInteger _availableCalled     = new AtomicInteger(0);
    private final AtomicInteger _cachedCalled        = new AtomicInteger(0);
    private final AtomicInteger _removedCalled       = new AtomicInteger(0);
    private final AtomicInteger _createdCalled       = new AtomicInteger(0);
    private final AtomicInteger _destroyedCalled   = new AtomicInteger(0);
    private final AtomicInteger _scanedCalled        = new AtomicInteger(0);
    private final AtomicInteger _touchedCalled       = new AtomicInteger(0);
    private final AtomicInteger _preciousCalled      = new AtomicInteger(0);
    private final AtomicInteger _needspaceCalled     = new AtomicInteger(0);
    private final AtomicInteger _stickyCalled        = new AtomicInteger(0);


    public void available(CacheRepositoryEvent event) {
        _availableCalled.getAndIncrement();
    }

    public void cached(CacheRepositoryEvent event) {
        _cachedCalled.getAndIncrement();
    }

    public void created(CacheRepositoryEvent event) {
        _createdCalled.getAndIncrement();
    }

    public void destroyed(CacheRepositoryEvent event) {
        _destroyedCalled.getAndIncrement();
    }

    public void needSpace(CacheNeedSpaceEvent event) {
        _needspaceCalled.getAndIncrement();
    }

    public void precious(CacheRepositoryEvent event) {
        _preciousCalled.getAndIncrement();
    }

    public void removed(CacheRepositoryEvent event) {
        _removedCalled.getAndIncrement();
    }

    public void scanned(CacheRepositoryEvent event) {
        _scanedCalled.getAndIncrement();
    }

    public void sticky(CacheRepositoryEvent event) {
        _stickyCalled.getAndIncrement();
    }

    public void touched(CacheRepositoryEvent event) {
        _touchedCalled.getAndIncrement();
    }

    public void actionPerformed(CacheEvent event) {

    }

    public int getAvailableCalled() {
        return _availableCalled.get();
    }

    public int getCachedCalled() {
        return _cachedCalled.get();
    }

    public int getRemovedCalled() {
        return _removedCalled.get();
    }

    public int getCreatedCalled() {
        return _createdCalled.get();
    }

    public int getDestroyedCalled() {
        return _destroyedCalled.get();
    }

    public int getScanedCalled() {
        return _scanedCalled.get();
    }

    public int getTouchedCalled() {
        return _touchedCalled.get();
    }

    public int getPreciousCalled() {
        return _preciousCalled.get();
    }

    public int getNeedspaceCalled() {
        return _needspaceCalled.get();
    }

    public int getStickyCalled() {
        return _stickyCalled.get();
    }

}