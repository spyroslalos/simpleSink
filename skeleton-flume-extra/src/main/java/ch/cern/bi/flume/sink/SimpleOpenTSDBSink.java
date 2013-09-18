/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ch.cern.bi.flume.sink;

import com.google.common.base.Preconditions;
import com.stumbleupon.async.Callback;
import org.apache.flume.Channel;

import net.opentsdb.core.TSDB;
import org.hbase.async.HBaseClient;

import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * <p>
 * A {@link Sink} implementation that simply inserts events to OpenTSDB.
 * </p>
 * <p>
 * <b>Configuration options</b>
 * </p>
 * <p>
 * <li>zkquorum = the host to send to</li>
 * <li>batchSize = the size of the batch</li>
 * </p>
 * <p>
 * <b>Metrics</b>
 * </p>
 * <p>
 * TODO
 * </p>
 */
public class SimpleOpenTSDBSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(SimpleOpenTSDBSink.class);

    private static final int DFLT_BATCH_SIZE = 100;
    private static final String DFLT_TABLE = "tsdb";
    private static final String DFLT_TABLE_UIDS = "tsdb-uid";

    private TSDB tsdb;

    private CounterGroup counterGroup;

    private int batchSize;
    private String zkquorum;
    private String seriesTable;
    private String uidsTable;

    private byte[] PUT = {'p', 'u', 't'};

    private String metric = null;
    private long timestamp;
    private float value;
    private Map<String, String> tags = new HashMap<String, String>();
    private HashMap<String, String> tags2 = new HashMap<String, String>();
    private final Callback<Exception, Exception> err = new ErrorCallback();

    public SimpleOpenTSDBSink() {
        counterGroup = new CounterGroup();
    }


    public boolean isFloat( String input )  
    {  
       try  
       {  
          Float.parseFloat( input );  
          return true;  
       }  
       catch( Exception e)  
       {  
          return false;  
       }  
    }  

    public String Parser(String event){
        
        String[] result = event.split("header");
        String[] result2 = result[0].split("body\":");
        /*here i have the body*/
        System.out.println(result2[1]);
        /*here i have the header*/
        System.out.println(result[1]);
        
        /* THIS IS THE HEADER */
        // replace multiple characters in string with BACKSPACE in HEADER
        logger.info("BODY ISSSS : " + result[1] + "AND I WILL CREATE A MAP FROM THE STRING");
        String Help_header = event.replaceAll("[\"\\{:}]", "");
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
        header = header.replaceAll("[']", ""); 
        event = header + " " + body; 
        
        logger.info("The hole Event is : " + event);        
        
        return event;
       
     	
    }
    
    @Override
    public void configure(Context context) {
        batchSize = context.getInteger("batchSize", DFLT_BATCH_SIZE);
        logger.info(this.getName() + " " +
                "batch size set to " + String.valueOf(batchSize));
        Preconditions.checkArgument(batchSize > 0, "Batch size must be > 0");

        zkquorum = context.getString("zkquorum");
        logger.info(this.getName() + " zkquorum set to " + zkquorum);

        seriesTable = context.getString("table.main", DFLT_TABLE);
        uidsTable = context.getString("table.uids", DFLT_TABLE_UIDS);
    }

    private final class ErrorCallback implements Callback<Exception, Exception> {
        public Exception call(final Exception arg) {
            logger.error(arg.getMessage());
            return arg;
        }
    }

    public int validEvent(final byte[] body) {
        int idx = 0;

        for (byte b : PUT) {
            if (body[idx++] != b)
                return -1;
        }
        return 0;
    }

    public void parser(String event) {
        String[] result = event.split(" ");
        String[] temp;

        this.metric = result[1];
        this.timestamp = Long.parseLong(result[2]);
	
	try{
            Float.valueOf(result[3]);
        }catch(Exception e){
            value = -1;
	    return;
        }
        this.value = Float.parseFloat(result[3]);
	
	
        for (int size = 4; size < result.length; size++) {
            temp = result[size].split("=");
            this.tags.put(temp[0], temp[1]);
        }
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
//      long eventCounter = counterGroup.get("events.success");

        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
            Event event = channel.take();
            // event != null && validEvent(event.getBody()) >= 0
            if ( event != null ) {
           	String even2 = new String(event.getBody());
	    	logger.info("LALALALALALLALALALAL!!L!LL!L!!!!!" + even2);
                logger.info(new String(event.getBody()));
		tags = event.getHeaders();
		Iterator iterator = tags.keySet().iterator();  
		int i = 1;
		// i will create the header and store the values....
		while (iterator.hasNext()) {
			  
   			String key = iterator.next().toString(); 
		 
   			String valueC = tags.get(key).toString();  
   		        if (i == 1){
				valueC = valueC.substring(0, valueC.length() - 3);
				this.timestamp = Long.parseLong(valueC);	
			}
			else if (i == 3)
			        this.metric = valueC;
			else if (i == 5){
				
        			this.value = Float.parseFloat(valueC);	
			}
   			logger.info(key + " " + valueC);
			logger.info("i ISSSSSS" + i);
  			i++;
		}
		// i will create both body and hashmap
		String helper = even2.replaceAll("[\"{}':,]", "");
		logger.info("BODY WITH ONLY GAPS IS" + " " + helper);
		String[] result = helper.split(" ");
		
		for (int size = 0; size < result.length; size+=2) {
            		logger.info("Prices ARE!!" + " " + result[size] + result[size+1]);
			this.tags2.put(result[size], result[size+1]);
        	}
		
		//
//	        char c = even2.charAt(0);
//		if ( c == '{' ) {
//               		String event_str = this.Parser( even2 );
//                	parser(event_str);
		logger.info(
                	"metric:" + metric +
                       	", timestamp: " + timestamp +
                       	", value: " + value);
    		if(value >= 0 )
                	tsdb.addPoint(metric, timestamp, value, tags2).addErrback(err);
            
//	    	}
	    }
	    tags.clear();
	    tags2.clear();
            transaction.commit();
            status = Status.READY;
        } catch (Throwable ex) {
            transaction.rollback();
            status = Status.BACKOFF;
            logger.error("Failed to deliver event. Exception follows.", ex);
            throw new EventDeliveryException("Failed to deliver event: " + ex);
        } finally {
            transaction.close();
        }
        return status;
    }

    @Override
    public void start() {
        logger.info("Starting {}...", this);
        HBaseClient hbaseClient = new HBaseClient(zkquorum);
        hbaseClient.setFlushInterval((short) 1000);

        try {
            hbaseClient.ensureTableExists(seriesTable)
                    .joinUninterruptibly();
            hbaseClient.ensureTableExists(uidsTable)
                    .joinUninterruptibly();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        tsdb = new TSDB(hbaseClient, seriesTable, uidsTable);

        counterGroup.setName(this.getName());

        super.start();
        logger.info("Null sink {} started.", getName());
    }

    @Override
    public void stop() {
        logger.info("Null sink {} stopping...", getName());

        super.stop();

        logger.info("Null sink {} stopped. Event metrics: {}",
                getName(), counterGroup);
    }

    @Override
    public String toString() {
        return "NullSink " + getName() + " { batchSize: " + batchSize + " }";
    }

}
