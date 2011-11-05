package org.dcache.chimera.migration;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class LoggingTerminableBlockingQueueDecorator<E> extends
        AbstractTerminableBlockingQueueDecorator<E> {

    public static final int IDS_PER_LINE = 1000;
    public static final int IDS_PER_DOT = 20;

    private final PrintStream _stream;
    private int _addedItemsCount = 0;
    private long _startTime = 0;
    private long _whenLastNewLine = 0;
    private final String _itemDescription;

    public LoggingTerminableBlockingQueueDecorator( TerminableBlockingQueue<E> queue,
                                                    PrintStream stream,
                                                    String itemDescription) {
        super( queue);
        _stream = stream;
        _itemDescription = itemDescription;
    }

    @Override
    public boolean add( E item) {
        super.add( item);
        itemAdded();
        return true;
    }

    @Override
    public boolean addAll( Collection<? extends E> c) {
        boolean itemsAdded;

        int initialSize = size();
        itemsAdded = super.addAll( c);
        int finalSize = size();

        for( int count = initialSize; count < finalSize; count++)
            itemAdded();

        return itemsAdded;
    }

    @Override
    public boolean offer( E item) {
        boolean addedItem = super.offer( item);
        if( addedItem)
            itemAdded();
        return addedItem;
    }

    @Override
    public void put( E item) throws InterruptedException {
        super.put( item);
        itemAdded();
    }

    @Override
    public void terminate() {
        super.terminate();
        emitFinalStatistics();
    }

    private void itemAdded() {
        if( _startTime == 0)
            _startTime = System.currentTimeMillis();

        if( _addedItemsCount % IDS_PER_LINE == 0) {
            long now = System.currentTimeMillis();
            if( _addedItemsCount > 0) {
                double freq = IDS_PER_LINE * 1000.0 / (now - _whenLastNewLine);
                _stream.println( " [ " + String.format( "%6.1f", freq) +
                                 " Hz]");
            }
            _whenLastNewLine = now;

            _stream.print( "Read " + String.format( "%6d", _addedItemsCount) +
                           " " + _itemDescription);
            _stream.print( ": ");
            _stream.flush();
        } else if( _addedItemsCount % IDS_PER_DOT == 0) {
            _stream.print( ".");
            _stream.flush();
        }

        _addedItemsCount++;
    }

    /**
     * Emit a final line describing the total number of items added.
     */
    public void emitFinalStatistics() {
        long elapsedTime = System.currentTimeMillis() - _startTime;

        double freq =
                _addedItemsCount /
                        (double) TimeUnit.SECONDS.convert( elapsedTime,
                                TimeUnit.MILLISECONDS);

        long elapsedDays =
            TimeUnit.DAYS.convert( elapsedTime, TimeUnit.MILLISECONDS);

        long elapsedTimeLessDays =
                elapsedTime -
                        TimeUnit.MILLISECONDS.convert( elapsedDays,
                                TimeUnit.DAYS);

        SimpleDateFormat format = new SimpleDateFormat( "HH:mm:ss");
        format.setTimeZone( TimeZone.getTimeZone( "GMT"));

        String days = elapsedDays > 1 ? "days" : "day";

        _stream.println( "\nIn total, checked " + _addedItemsCount + " " +
                         _itemDescription + " in " +
                         (elapsedDays > 0 ? elapsedDays + " " + days + " " : "") +
                         format.format( new Date( elapsedTimeLessDays)) + " [" +
                         String.format( "%.1f", freq) + " Hz]");
    }

}
