/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package ch.cern.bi.flume.sink;

import com.stumbleupon.async.Callback;
import net.opentsdb.core.TSDB;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.channel.PseudoTxnMemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.lifecycle.LifecycleException;
import org.hbase.async.HBaseClient;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.HashMap;

public class TestSimpleOpenTSDBSinkSink {

    private static final Logger logger = LoggerFactory
            .getLogger(TestSimpleOpenTSDBSinkSink.class);

    private static final String ZKQUORUM = "188.184.132.6:2181";
    private SimpleOpenTSDBSink sink;

    @Before
    public void setUp() {
        sink = new SimpleOpenTSDBSink();
    }

    /**
     * Test something.
     */
    @Test
    public void testSomething() throws InterruptedException, LifecycleException,
            EventDeliveryException {
        // test something
    }

    /**
     * Test lifecycle.
     */
    @Test
    public void testLifeCycle() throws InterruptedException, LifecycleException,
            EventDeliveryException {

        Context context = new Context();

        context.put("sink.zkquorum", ZKQUORUM);

        Configurables.configure(sink, context);

        Channel channel = new PseudoTxnMemoryChannel();
        Configurables.configure(channel, context);

        sink.setChannel(channel);
        sink.start();

        sink.stop();
    }

    /**
     * Test writing to opentsdb.
     */
    @Test
    public void testWriteToOpenTSDB() throws InterruptedException, LifecycleException,
            EventDeliveryException {

        Context context = new Context();

        context.put("zkquorum", ZKQUORUM);

        Configurables.configure(sink, context);

        Channel channel = new PseudoTxnMemoryChannel();
        Configurables.configure(channel, context);

        sink.setChannel(channel);
        sink.start();
        Event event = EventBuilder.withBody(
                "put batchhosts.run 1376905539 12 hostname=lxbsq2919 status=closed_Full prod=1",
                Charset.forName("UTF-8"));

        channel.put(event);

        sink.process();

        sink.stop();
    }

    private final class ErrorCallback implements Callback<Exception, Exception> {
        public Exception call(final Exception arg) {
            logger.error(arg.getMessage());
            return arg;
        }
    }

    private final Callback<Exception, Exception> err = new ErrorCallback();

    /**
     * Test writing to opentsdb.
     */
    @Test
    public void testStandaloneWriteToOpenTSDB() throws InterruptedException, LifecycleException,
            EventDeliveryException {
        String seriesTable = "tsdb";
        String uidsTable = "tsdb-uid";
        HBaseClient hbaseClient = new HBaseClient(ZKQUORUM);
        hbaseClient.setFlushInterval((short) 1000);

        try {
            hbaseClient.ensureTableExists(seriesTable)
                    .joinUninterruptibly();
            hbaseClient.ensureTableExists(uidsTable)
                    .joinUninterruptibly();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        TSDB tsdb = new TSDB(hbaseClient, seriesTable, uidsTable);

        HashMap<String, String> tags = new HashMap<String, String>();
        tags.put("hostname", "lxbsq2919");
        tsdb.addPoint("batchhosts.run", 1376905539, 12.0, tags);
        tsdb.flush();
    }

}
