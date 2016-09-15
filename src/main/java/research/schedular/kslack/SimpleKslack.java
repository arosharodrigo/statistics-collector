package research.schedular.kslack;

import org.wso2.carbon.databridge.commons.Event;
import research.schedular.eventreceiver.WSO2EventReceiver;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by sajith on 8/30/16.
 */
public class SimpleKslack {
    public static final long BUFFER_FLUSH_INTERVAL = 20 * 1000l;
    public static final int TIME_STAMP_FIELD = 0;

    public  WSO2EventReceiver receiver;

    public SimpleKslack(WSO2EventReceiver eventReceiver){
        this.receiver = eventReceiver;
    }

    Lock bufferLock = new ReentrantLock();
    public Map<Long, Event> buffer = new TreeMap();
    Timer bufferFlushTimer = null;

    class BufferFlushTask extends TimerTask {

        @Override
        public void run() {
            System.out.println("Flushing " + buffer.size() + " events in KSlack buffer");

            List<Event> eventList = new ArrayList<Event>();
            bufferLock.lock();
            for (Map.Entry entry : buffer.entrySet()){
                eventList.add((Event)entry.getValue());
            }
            buffer.clear();
            bufferLock.unlock();

            receiver.onReceive(eventList);
        }
    }

    public void addEvents(List<Event> eventList){
        if (bufferFlushTimer == null){
            bufferFlushTimer = new Timer();
            bufferFlushTimer.schedule(new BufferFlushTask(), 0, BUFFER_FLUSH_INTERVAL);
            System.out.println("Starting buffer timer.....");
        }

        bufferLock.lock();
        for (Event event : eventList){
            Long sequenceNo = (Long) event.getPayloadData()[TIME_STAMP_FIELD];
            buffer.put(sequenceNo, event);
        }
        bufferLock.unlock();
    }
}
