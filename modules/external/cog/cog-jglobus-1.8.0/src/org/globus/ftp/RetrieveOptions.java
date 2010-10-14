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
package org.globus.ftp;

/**
   Options to the command RETR, as defined in GridFTP.
   In client-server transfers, this implementation does not 
   support starting/min/max parallelism. All three values must be equal.
   In third party transfers, this is not necessary.
 */
public class RetrieveOptions extends Options {

    protected int startParallelism;
    protected int minParallelism;
    protected int maxParallelism;
	
    public RetrieveOptions() {
	this(1);
    }

    /**
       @param parallelism required min, max, and starting parallelism 
    */
    public RetrieveOptions(int parallelism) {
	super("RETR");
	this.startParallelism = parallelism;
	this.minParallelism = parallelism;
	this.maxParallelism = parallelism;
    }

    /**
       Use only in third party mode.
     */
    public void setStartingParallelism(int startParallelism) {
	this.startParallelism = startParallelism;
    }

    /**
       Use only in third party mode.
     */
    public void setMinParallelism(int minParallelism) {
	this.minParallelism = minParallelism;
    }
    
    /**
       Use only in third party mode.
     */
    public void setMaxParallelism(int maxParallelism) {
	this.maxParallelism = maxParallelism;
    }

    public int getStartingParallelism() {
	return this.startParallelism;
    }

    public int getMinParallelism() {
	return this.minParallelism;
    }
    
    public int getMaxParallelism() {
	return this.maxParallelism;
    }
    
    public String getArgument() {
	return "Parallelism=" + startParallelism + "," +
	    minParallelism + "," + maxParallelism + ";";
    }

}

    
