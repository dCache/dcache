package org.dcache.pool.statistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Collectors;

public class Statistics {

    private long _readRequestNum;
    private long _readBytes;
    private ArrayList<Double> _readSpeeds;
    private long _minReadSpeed;
    private long _maxReadSpeed;
    private long _avgReadSpeed;
    private long _95ReadSpeed;
    private long _totalReadTime;

    private long _writeRequestNum;
    private long _writeBytes;
    private ArrayList<Double> _writeSpeeds;
    private long _minWriteSpeed;
    private long _maxWriteSpeed;
    private long _avgWriteSpeed;
    private long _95WriteSpeed;
    private long _totalWriteTime;

    public void updateRead(long readBytes, long readTime){
        _readRequestNum ++;
        _readBytes += readBytes;
        _readSpeeds.add(calculateSpeed(readBytes, readTime));
        _totalReadTime += readTime;
    }

    public void writeRead(long writeBytes, long writeTime){
        _writeRequestNum ++;
        _writeBytes += writeBytes;
        _writeSpeeds.add(calculateSpeed(writeBytes, writeTime));
        _totalWriteTime += writeTime;
    }

    private double calculateSpeed(long readBytes, long readTime){
        return readBytes / readTime;
    }

    public Double getMinReadSpeed(){
        return Collections.min(_readSpeeds);
    }

    public Double getMaxReadSpeed(){
        return Collections.max(_readSpeeds);
    }

    /*public Double getAvgReadSpeed(){
        return _readSpeeds.stream().collect(Collectors.groupingBy(
                d -> d.name,
                Collectors.summarizingInt(d -> d.value)))
                .forEach((name, summary) -> System.out.println(name + ": " + summary));
    }*/
}
