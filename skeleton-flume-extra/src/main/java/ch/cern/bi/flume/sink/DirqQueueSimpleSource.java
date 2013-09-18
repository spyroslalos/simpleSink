package ch.cern.bi.flume.sink;

import ch.cern.aimon.flume.source.dirq.*;
import ch.cern.dirq.Queue;
import ch.cern.dirq.QueueNull;
import ch.cern.dirq.QueueSimple;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This is a Directory Queue source for Apache Flume.
 *
 * It iterated through the configured Directory Queue elements,
 * extract the events and send them to the Flume channel.
 *
 * Different modes are supported:
 *
 * <li></><b>raw:</b> the entire content of a dirq element is put into an Event body
 * <li></><b>mq:</b> the dirq elements are interpreted as a Message Queue (simplified)
 * so headers and body are mapped to relatives Event headers and body
 * <li></><b>lemonmetrics:</b> the dirq elements are expected to be Message Queue
 * elements coming from lemon-forwarder. The body payload is extracted and samples
 * are divided into multiple events, headers are mapped to Event headers.
 *
 * Parameters available:
 *
 * <li>type = ch.cern.aimon.flume.source.DirqQueueSimpleSource</li>
 * <li>path = the path to the dirq to be used as source, required</li>
 * <li>mode = raw|mq|lemonmetrics [default = raw]</li>
 * <li>purgeInterval = the purge interval for the source directory queue
 * [default = 300]</li>
 * <li>remove = true|false if elements should be removed from the
 * directory queue [default = true]</li>
 * <li>sleepTime = time to sleep if the directory queue is found empty,
 * this is to avoid cpu spinning to 100% for polling [default = 3]</li>
 * <li>rejectedPath = null|/path/to/rejected/dirq if remove is set to true
 * and dirq elements are not valid they are moved to the configured rejected
 * directory queue. If null is given rejected elements are dropped
 * [default = null]</li>
 * <li>batchSize = the maximum number of items to send to the channel in
 * one operation [default = 50]</li>
 *
 * User: Massimo Paladin
 * Date: 5/3/13
 * Time: 10:53 AM
 */
public class DirqQueueSimpleSource extends AbstractSource implements Configurable, PollableSource {
    private static final Logger logger = LoggerFactory
            .getLogger(DirqQueueSimpleSource.class);

    private static final int MAX_ELEMENTS = 1000;
    private static final List<Event> EMPTY_EVENTS_LIST =
            Collections.unmodifiableList(new LinkedList<Event>());

    private static final int DEFAULT_PURGE_INTERVAL = 300;
    private static final String DEFAULT_MODE = "raw";
    private static final boolean DEFAULT_REMOVE = true;
    private static final int DEFAULT_SLEEP_TIME = 3;
    private static final String DEFAULT_REJECTED_PATH = "null";
    private static final int DEFAULT_BATCH_SIZE = 50;

    private int purgeInterval;
    private String queuePath;
    private String mode;
    private String rejectedPath;
    private boolean remove;
    private int sleepTime;
    private int batchSize;

    private Queue dirq;
    private Queue rejected;
    private long nextPurge;
    private EventFactory eventFactory;
    
    
    public String Parser(String event){
        
        String[] result = event.split("header");
        String[] result2 = result[0].split("body\":");
        /*here i have the body*/
        System.out.println(result2[1]);
        /*here i have the header*/
        System.out.println(result[1]);
        
        /* THIS IS THE HEADER */
        // replace multiple characters in string with BACKSPACE in HEADER
        logger.info("HEADER ISSSS : " + result[1]);
        String Help_header = result[1].replaceAll("[\"{}]", "");
        System.out.println(Help_header);
        Help_header = Help_header.replaceAll(" ","");
        System.out.println(Help_header);
        Help_header = Help_header.replaceAll("[:,]"," ");
        System.out.println(Help_header);
        String[] headerArr = Help_header.split(" +");
        for ( int i=0; i < headerArr.length ; i++){
		
		logger.info("Array of " + i + headerArr[i]);

	}
	logger.info("XAXAXa" + Help_header);
        String header = "put ";
	logger.info("4444 is " + headerArr[4]);
        header = header + headerArr[4] + " " + headerArr[2] + " " + headerArr[6] ;
        
	logger.info("ZEEEEEEEEEE1111111111" + header);
	/*
        for (int i=0 ; i<headerArr.length ; i +=2){
        	if (headerArr[i]=="")
        	logger.info(headerArr[i] + " XOXOXOXO " + headerArr[i+1]);
        	header +=  headerArr[i] + "=" + headerArr[i+1] + " ";     
        }
        */
        System.out.println("Finally the header is :" + header);
        
        /* THIS IS THE BODY */
	logger.info("BODYYYY ISSSS : " + result2[1]);
        Help_header = result2[1].replaceAll("[\"{}' ]", "");
        System.out.println(Help_header);
        Help_header = Help_header.replaceAll(" ","");
        System.out.println(Help_header);
        Help_header = Help_header.replaceAll("[:,]"," ");
        System.out.println(Help_header);
        headerArr = Help_header.split(" +");
        logger.info("ZEEEEEEEEEEEEEEEEEEE" + Help_header);
        System.out.println(headerArr.length);
        String body = "";
        
        for (int i=0 ; i<headerArr.length ; i +=2){
        	if (headerArr[i]=="")
        	logger.info(headerArr[i] + " XOXOXOXO " + headerArr[i+1]);
        	body +=  headerArr[i] + "=" + headerArr[i+1] + " ";     
        }
        logger.info("Finally the body is :" + body);
        
        event = header + " " + body; 
        
        logger.info("The hole Event is : " + event);        
        
        return event;
       
     	
    }

    @Override
    public void configure(Context context) {
        String queuePath = context.getString("path", "/d4");
        int purgeInterval = context.getInteger("purgeInterval", DEFAULT_PURGE_INTERVAL);
        String mode = context.getString("mode", DEFAULT_MODE);
        boolean remove = context.getBoolean("remove", DEFAULT_REMOVE);
        int sleepTime = context.getInteger("sleepTime", DEFAULT_SLEEP_TIME);
        String rejectedPath = context.getString("rejectedPath", DEFAULT_REJECTED_PATH);
        int batchSize = context.getInteger("batchSize", DEFAULT_BATCH_SIZE);

        this.queuePath = queuePath;
        this.purgeInterval = purgeInterval;
        this.mode = mode;
        this.rejectedPath = rejectedPath;
        this.remove = remove;
        this.sleepTime = sleepTime;
        this.batchSize = batchSize;

        if (mode.equals("raw")) {
            eventFactory = new RawEventFactory();
        } else if (mode.equals("mq")) {
            eventFactory = new MQEventFactory();
        } else if (mode.equals("lemonmetrics")) {
            eventFactory = new LemonMetricsEventFactory();
        } else {
            throw new IllegalArgumentException(
                    "Dirq source " + getName() + " unexpected mode: " + mode + ".");
        }
    }

    @Override
    public void start() {
//	logger.info("AXXAXAXAXAXA");
        try {
            dirq = new QueueSimple(queuePath);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Error initializing dirq at path " +
                            queuePath + ": " + e.getMessage());
        }
        if (rejectedPath.equals("null")) {
            rejected = new QueueNull();
        } else {
            try {
                rejected = new QueueSimple(rejectedPath);
            } catch (IOException e) {
                throw new RuntimeException(
                        "Error initializing rejected dirq at path " +
                                rejectedPath + ": " + e.getMessage());
            }
        }
        logger.info("Dirq source " + getName() + " has started.");
    }

    @Override
    public void stop() {
        logger.info("Dirq source " + getName() + " has stopped.");
    }

    @Override
    public Status process() throws EventDeliveryException {
        logger.debug("Dirq source " + getName() + " process called.");
        maybePurge();
        ChannelProcessor channelProcessor = getChannelProcessor();

        try {
            maybeSleep();
        } catch (InterruptedException e) {
            return Status.READY;
        }
        logger.debug("Dirq source " + getName() + " has " + dirq.count() + " messages.");
        int counter = 0;
        for (String element : dirq) {
            if (counter++ == MAX_ELEMENTS) {
                logger.debug("Dirq source " + getName() + " max elements reached, returning.");
                return Status.READY;
            }
            try {
                if (!dirq.lock(element)) {
                    logger.debug("Dirq source " + getName() + " cannot lock, continue.");
                    continue;
               }
            } catch (IOException e) {
            	logger.warn("Dirq source " + getName() + " locking exception: ", e);
	      	continue;  // we can simply skip it for now
            }
            logger.info("Am gonna parse the Event : " + dirq.get(element) );
  //          logger.info("OPAPAPAPAPAPAPAPAPAP" + this.Parser(dirq.get(element)) );
            element = element.replace("\\\"", "\"" );
//	    dirq.set(element);            
            logger.info("9999999999999999999999999999999999999999999 " + dirq.get(element) );

            logger.debug("Dirq source " + getName() + " handling message: " + element);
            byte[] content = dirq.getAsByteArray(element);
            List<Event> events = extractEvents(element, content);
            if (events.isEmpty()) {
                logger.debug("Dirq source " + getName() + " empty message: " + element);
                continue;
            }
            for (Event el:events)
                logger.debug(new String(el.getBody()));
            logger.debug("Dirq source " + getName() + " extracted from one message events: " + events.size());
            Status processed = processEvents(channelProcessor, element, events);
            if (processed.equals(Status.BACKOFF)) {
                logger.debug("Dirq source " + getName() + " backing off.");
                return Status.BACKOFF;
            }

	}
        return Status.READY;
    }

    /**
     * Extract the events from the given dirq element.
     * @param element the element under process
     * @param content the element dirq content to be processed
     * @return the List of Event extracted
     */
    private List<Event> extractEvents(String element, byte[] content) {
        List<Event> events;
        try {
            events = eventFactory.getEvents(content);
        } catch (Throwable t) {
            if (remove) {
                try {
                    String newEl = rejected.add(content);
                    logger.warn(
                            "Dirq bad message, added to rejected: " + newEl, t);
                    dirq.remove(element);
                } catch (IOException e) {
                    logger.warn(
                            "Dirq rejected message could not be added " +
                                    "to rejected queue, leaving it in the queue: " +
                                    element, e);
                }
            } else {
                logger.warn("Dirq message rejected: " + element, t);
                try {
                    dirq.unlock(element);
                } catch (IOException e) {
                    // simply ignoring it, purge will fix it soon
                }
            }
            return EMPTY_EVENTS_LIST;
        }
        return events;
    }

    /**
     * Dispatch the events to the channel.
     * @param channelProcessor the channel processor to which events should be sent
     * @param element the dirq element name under process
     * @param events the events to be dispatched to the channel
     * @return Status.READY if everything is fine, Status.BACKOFF if an exception
     * happens during dispatch
     */
    private Status processEvents(ChannelProcessor channelProcessor,
                                 String element, List<Event> events) {
        try {
            int eventsSize = events.size();
            for (int i = 0; i < eventsSize; i += batchSize) {
                channelProcessor.processEventBatch(
                        events.subList(i, Math.min(i + batchSize, eventsSize)));
            }
            if (remove)
                dirq.remove(element);
            else
                dirq.unlock(element);
        } catch (Throwable t) {
            try {
                dirq.unlock(element);
            } catch (IOException e) {
                // simply ignoring it, purge will fix it soon
            }
            logger.error("Dirq source " + getName() +
                    ": Error sending to the channel. " +
                    "Exception follows.", t);
            if (t instanceof Error) {
                throw (Error) t;
            }
            return Status.BACKOFF;
        }
        return Status.READY;
    }

    /**
     * Purge the directory queue every purgeInterval seconds.
     */
    private void maybePurge() {
        if (System.currentTimeMillis() > nextPurge) {
            try {
                dirq.purge();
                logger.debug("Dirq source " + getName() + " purged.");
                nextPurge = System.currentTimeMillis() + (purgeInterval * 1000);
            } catch (IOException e) {
                logger.warn("Dirq source " + getName() + ": Unable to purge dirq. " +
                        "Exception follows.", e);
            }
        }
    }

    /**
     * If the directory queue is empty sleep for sleepTime seconds
     * @throws InterruptedException due to sleep
     */
    private void maybeSleep() throws InterruptedException {
        if (dirq.count() == 0) {
            logger.debug("sleeping " + sleepTime + " seconds");
            TimeUnit.SECONDS.sleep(sleepTime);
        }
    }
}
