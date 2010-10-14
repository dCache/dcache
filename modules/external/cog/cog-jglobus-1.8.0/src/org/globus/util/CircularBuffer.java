/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.util;

public class CircularBuffer {
	
    protected Object[] buf;
    protected int in = 0;
    protected int out= 0;
    protected int count= 0;
    protected int size;
    
    protected boolean interruptPut = false;
    protected boolean interruptGet = false;
    protected boolean closePut = false;

    public CircularBuffer(int size) {
	this.size = size;
	buf = new Object[size];
    }
    
    public synchronized boolean isEmpty() {
	return (this.count == 0);
    }

    public synchronized boolean put(Object o) 
	throws InterruptedException {
	if (this.interruptPut) {
	    return false;
	}
	while (count==size) {
	    wait();
	    if (this.interruptPut) {
		return false;
	    }
	}
	buf[in] = o;
	++count;
	in=(in+1) % size;
	notify();
	return true;
    }
    
    public synchronized Object get() 
	throws InterruptedException {
	if (this.interruptGet) {
	    return null;
	}
	while (count==0) {
	    if (this.closePut) {
		return null;
	    }
	    wait();
	    if (this.interruptGet) {
		return null;
	    }
	}
	Object o =buf[out];
	buf[out]=null;
	--count;
	out=(out+1) % size;
	notify();
	return (o);
    }
    
    public synchronized void closePut() {
	this.closePut = true;
	notifyAll();
    }

    public synchronized boolean isPutClosed() {
	return this.closePut;
    }

    public synchronized void interruptBoth() {
	this.interruptGet = true;
	this.interruptPut = true;
	notifyAll();
    }
    
    public synchronized void interruptGet() {
	this.interruptGet = true;
	notifyAll();
    }

    public synchronized void interruptPut() {
	this.interruptPut = true;
	notifyAll();
    }

    public synchronized boolean isGetInterrupted() {
	return this.interruptGet;
    }

    public synchronized boolean isPutInterrupted() {
	return this.interruptPut;
    }
    
}
