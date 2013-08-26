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

import java.util.HashMap;


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
    private HashMap<String, String> tags = new HashMap<String, String>();

    private final Callback<Exception, Exception> err = new ErrorCallback();

    public SimpleOpenTSDBSink() {
        counterGroup = new CounterGroup();
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

            if (event != null && validEvent(event.getBody()) >= 0) {
                logger.info(new String(event.getBody()));
                parser(new String(event.getBody()));
                logger.info(
                        "metric:" + metric +
                        ", timestamp: " + timestamp +
                        ", value: " + value);
    		if(value >= 0 )
                	tsdb.addPoint(metric, timestamp, value, tags).addErrback(err);
                tags.clear();
            }
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
