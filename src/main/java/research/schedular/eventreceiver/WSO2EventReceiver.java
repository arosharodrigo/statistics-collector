package research.schedular.eventreceiver;

/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import com.google.common.base.Splitter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.log4j.Logger;
import org.wso2.carbon.databridge.commons.Credentials;
import org.wso2.carbon.databridge.commons.Event;
import org.wso2.carbon.databridge.commons.StreamDefinition;
import org.wso2.carbon.databridge.core.AgentCallback;
import org.wso2.carbon.databridge.core.DataBridge;
import org.wso2.carbon.databridge.core.Utils.AgentSession;
import org.wso2.carbon.databridge.core.definitionstore.AbstractStreamDefinitionStore;
import org.wso2.carbon.databridge.core.definitionstore.InMemoryStreamDefinitionStore;
import org.wso2.carbon.databridge.core.exception.DataBridgeException;
import org.wso2.carbon.databridge.core.exception.StreamDefinitionStoreException;
import org.wso2.carbon.databridge.core.internal.authentication.AuthenticationHandler;
import org.wso2.carbon.databridge.receiver.binary.conf.BinaryDataReceiverConfiguration;
import org.wso2.carbon.databridge.receiver.binary.internal.BinaryDataReceiver;
import org.wso2.carbon.databridge.receiver.thrift.ThriftDataReceiver;
import org.wso2.carbon.user.api.UserStoreException;
import org.wso2.siddhi.extension.he.api.HomomorphicEncDecService;
import research.schedular.StatisticsCollector;
import research.schedular.Util;
import research.schedular.kslack.SimpleKslack;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class WSO2EventReceiver {
    Logger log = Logger.getLogger(WSO2EventReceiver.class);
    ThriftDataReceiver thriftDataReceiver;
    BinaryDataReceiver binaryDataReceiver;
    AbstractStreamDefinitionStore streamDefinitionStore = new InMemoryStreamDefinitionStore();
    static final WSO2EventReceiver testServer = new WSO2EventReceiver();
    AtomicInteger count = new AtomicInteger(0);
    List<Double> latencyValues = new ArrayList<Double>();
    List<Double> heLatencyValues = new ArrayList<Double>();

    Lock latencyValuesLock = new ReentrantLock();
    Lock heLatencyValuesLock = new ReentrantLock();

    SimpleKslack kslack = new SimpleKslack(this);
    private boolean isUsingKslack = false;

    private long lastTimeStamp = 0;
    private long outOfOrderEventCount = 0;
    private long nonOutOfOrderEventCount = 0;

    private static final String FIELD_SEPARATOR = "###";
    private static final String COMMA_SEPARATOR = ",";

    private static final int batchSize = 478;

    // For Email flow
    private static final int maxEmailLength = 40;
    private static final int compositeEventSize = 10;

    // For EDGAR flow
    private static final int maxEdgarLength = 20;
    private static final int compositeEdgarEventSize = 23;

    public static HomomorphicEncDecService homomorphicEncDecService;
    private static ExecutorService decodingWorkers;

    private Splitter fieldSplitter = Splitter.on(FIELD_SEPARATOR);
    private Splitter commaSplitter = Splitter.on(COMMA_SEPARATOR);

    public static WSO2EventReceiver getInstance(){
        return testServer;
    }

    public int getAndResetCount(){
        int value = count.getAndSet(0);
        return value;
    }

    public double[] getAndResetLatencyValues(){
        latencyValuesLock.lock();
        double[] values = new double[latencyValues.size()];


        for (int i = 0; i < latencyValues.size(); i++){
            values[i] = latencyValues.get(i);
        }
        latencyValues.clear();
        latencyValuesLock.unlock();

        return values;
    }

    public double[] getAndResetHeLatencyValues(){
        heLatencyValuesLock.lock();
        double[] values = new double[heLatencyValues.size()];


        for (int i = 0; i < heLatencyValues.size(); i++){
            values[i] = heLatencyValues.get(i);
        }
        heLatencyValues.clear();
        heLatencyValuesLock.unlock();

        return values;
    }

    /*public void onReceive(List<Event> eventList){
        totalCount += eventList.size();
        count.addAndGet(eventList.size());
        latencyValuesLock.lock();
        heLatencyValuesLock.lock();
        for (Event event : eventList){
            double latency = (System.currentTimeMillis() - event.getTimeStamp());
            latencyValues.add(latency);
            if(event.getStreamId().contains("outputHEEmailsStream")) {
                heLatencyValues.add(latency);
            }
//            checkOutOfOrder(event.getTimeStamp());
        }
        heLatencyValuesLock.unlock();
        latencyValuesLock.unlock();
    }*/

    public void onReceive(List<Event> eventList){
        for (final Event event : eventList) {
            if(event.getStreamId().contains("outputHEEmailsStream")) {
                decodingWorkers.submit(new Runnable() {
                    public void run() {
                        decodeCompositeEvent(event);
                    }
                });
            } else if(event.getStreamId().contains("outputEdgarStream")) {
                count.incrementAndGet();
                double latency = (System.currentTimeMillis() - event.getTimeStamp());
                latencyValuesLock.lock();
                latencyValues.add(latency);
                latencyValuesLock.unlock();
            } else if(event.getStreamId().contains("outputHEEdgarStream")) {
                decodingWorkers.submit(new Runnable() {
                    public void run() {
                        decodeCompositeEventEdgar(event);
                    }
                });
            } else {
                count.incrementAndGet();
                Object[] payloadData = event.getPayloadData();
                double latency = (System.currentTimeMillis() - (Long)payloadData[0]);
                latencyValuesLock.lock();
                latencyValues.add(latency);
                latencyValuesLock.unlock();
            }
        }
    }

    private void decodeCompositeEvent(Event event) {
        try {
            List<Event> decodedEvents = new ArrayList<Event>();
            Object[] payloadData = event.getPayloadData();

            String field1 = (String) payloadData[0];
            Iterator<String> field1Arr = fieldSplitter.split(field1).iterator();

            String field5 = (String) payloadData[4];
            Iterator<String> field5Arr = fieldSplitter.split(field5).iterator();

            String field6 = (String) payloadData[5];
            Iterator<String> field6Arr = fieldSplitter.split(field6).iterator();

            String field7 = (String) payloadData[6];
            Iterator<String> field7Arr = fieldSplitter.split(field7).iterator();

            String field2 = (String) payloadData[1];
            String decryptedField2 = homomorphicEncDecService.decryptLongVector(field2);
            Iterator<String> decryptedField2Arr = commaSplitter.split(decryptedField2).iterator();

            String field3 = (String) payloadData[2];
            String decryptedField3 = homomorphicEncDecService.decryptLongVector(field3);
            Iterator<String> decryptedField3Arr = commaSplitter.split(decryptedField3).iterator();

            String field4 = (String) payloadData[3];
            String decryptedField4 = homomorphicEncDecService.decryptLongVector(field4);
            Iterator<String> decryptedField4Arr = commaSplitter.split(decryptedField4).iterator();

            for(int i = 0; i < compositeEventSize; i++) {
                StringBuilder field2Builder = new StringBuilder();
                StringBuilder field3Builder = new StringBuilder();
                StringBuilder field4Builder = new StringBuilder();
                for(int j = 0; j < maxEmailLength; j++) {
                    field2Builder.append(decryptedField2Arr.next());
                    field3Builder.append(decryptedField3Arr.next());
                    field4Builder.append(decryptedField4Arr.next());
                }

                String f5Val = (field5Arr.hasNext()) ? field5Arr.next() : "";
                String f6Val = (field6Arr.hasNext()) ? field6Arr.next() : "";
                String f7Val = (field7Arr.hasNext()) ? field7Arr.next() : "";

                String splitField1 = field1Arr.next();
                Object[] payloadDataArray = {
                        Long.valueOf(splitField1),
                        field2Builder.toString(),
                        field3Builder.toString(),
                        field4Builder.toString(),
                        f5Val,
                        f6Val,
                        f7Val
                };
                Event decodedEvent = new Event(event.getStreamId(), Long.valueOf(splitField1), null, null, payloadDataArray);
                String f2 = field2Builder.toString().replace("0", ""); // Try later: boolean isSatisfiedF2 = field2Builder.toString().equals("0000000000000000000000000000000000000000");
                String f3 = field3Builder.toString().replace("0", "");
                String f4 = field4Builder.toString().replace("0", "");
                if(f2.isEmpty() && (f3.isEmpty() || f4.isEmpty())) {
//                            log.info("Filtered out event [" + decodedEvent + "]");
                } else {
                    decodedEvents.add(decodedEvent);
                }
            }
            for(Event decodedEvent: decodedEvents) {
                count.incrementAndGet();
                double latency = (System.currentTimeMillis() - decodedEvent.getTimeStamp());
                latencyValuesLock.lock();
                heLatencyValuesLock.lock();
                latencyValues.add(latency);
                heLatencyValues.add(latency);
                heLatencyValuesLock.unlock();
                latencyValuesLock.unlock();
            }
        } catch (Throwable th) {
            log.error("Error occurred while decoding [" + th + "], Event [" + event + "]");
            th.printStackTrace();
        }
    }

    private void decodeCompositeEventEdgar(Event event) {
        try {
            List<Event> decodedEvents = new ArrayList<Event>();
            Object[] payloadData = event.getPayloadData();

            String field1 = (String) payloadData[0];
            Iterator<String> field1Arr = fieldSplitter.split(field1).iterator();

            String field2 = (String) payloadData[1];
            Iterator<String> field2Arr = fieldSplitter.split(field2).iterator();

            String field4 = (String) payloadData[3];
            Iterator<String> field4Arr = fieldSplitter.split(field4).iterator();

            String field5 = (String) payloadData[4];
            Iterator<String> field5Arr = fieldSplitter.split(field5).iterator();

            String field6 = (String) payloadData[5];
            Iterator<String> field6Arr = fieldSplitter.split(field6).iterator();

            String field7 = (String) payloadData[6];
            Iterator<String> field7Arr = fieldSplitter.split(field7).iterator();

            String field10 = (String) payloadData[9];
            Iterator<String> field10Arr = fieldSplitter.split(field10).iterator();

            String field11 = (String) payloadData[10];
            Iterator<String> field11Arr = fieldSplitter.split(field11).iterator();

            String field12 = (String) payloadData[11];
            Iterator<String> field12Arr = fieldSplitter.split(field12).iterator();

            String field13 = (String) payloadData[12];
            Iterator<String> field13Arr = fieldSplitter.split(field13).iterator();

            String field14 = (String) payloadData[13];
            Iterator<String> field14Arr = fieldSplitter.split(field14).iterator();

            String field15 = (String) payloadData[14];
            Iterator<String> field15Arr = fieldSplitter.split(field15).iterator();

            String field16 = (String) payloadData[15];
            Iterator<String> field16Arr = fieldSplitter.split(field16).iterator();

            String field3 = (String) payloadData[2];
            String decryptedField3 = homomorphicEncDecService.decryptLongVector(field3);
            Iterator<String> decryptedField3Arr = commaSplitter.split(decryptedField3).iterator();

            String field8 = (String) payloadData[7];
            String decryptedField8 = homomorphicEncDecService.decryptLongVector(field8);
            Iterator<String> decryptedField8Arr = commaSplitter.split(decryptedField8).iterator();

            String field9 = (String) payloadData[8];
            String decryptedField9 = homomorphicEncDecService.decryptLongVector(field9);
            Iterator<String> decryptedField9Arr = commaSplitter.split(decryptedField9).iterator();


            for(int i = 0; i < compositeEdgarEventSize; i++) {
                StringBuilder field3Builder = new StringBuilder();
                StringBuilder field8Builder = new StringBuilder();
                StringBuilder field9Builder = new StringBuilder();
                for(int j = 0; j < maxEdgarLength; j++) {
                    field3Builder.append(decryptedField3Arr.next());
                    field8Builder.append(decryptedField8Arr.next());
                    field9Builder.append(decryptedField9Arr.next());
                }

                String f2Val = (field2Arr.hasNext()) ? field2Arr.next() : "";
                String f4Val = (field4Arr.hasNext()) ? field4Arr.next() : "";
                String f5Val = (field5Arr.hasNext()) ? field5Arr.next() : "";
                String f6Val = (field6Arr.hasNext()) ? field6Arr.next() : "";
                String f7Val = (field7Arr.hasNext()) ? field7Arr.next() : "";
                String f10Val = (field10Arr.hasNext()) ? field10Arr.next() : "";
                String f11Val = (field11Arr.hasNext()) ? field11Arr.next() : "";
                String f12Val = (field12Arr.hasNext()) ? field12Arr.next() : "";
                String f13Val = (field13Arr.hasNext()) ? field13Arr.next() : "";
                String f14Val = (field14Arr.hasNext()) ? field14Arr.next() : "";
                String f15Val = (field15Arr.hasNext()) ? field15Arr.next() : "";
                String f16Val = (field16Arr.hasNext()) ? field16Arr.next() : "";

                String splitField1 = field1Arr.next();
                Object[] payloadDataArray = {
                        Long.valueOf(splitField1),
                        f2Val,
                        field3Builder.toString(),
                        f4Val,
                        f5Val,
                        f6Val,
                        f7Val,
                        field8Builder.toString(),
                        field9Builder.toString(),
                        f10Val,
                        f11Val,
                        f12Val,
                        f13Val,
                        f14Val,
                        f15Val,
                        f16Val
                };
                Event decodedEvent = new Event(event.getStreamId(), event.getTimeStamp(), null, null, payloadDataArray);
                String f3 = field3Builder.toString().replace("0", ""); // Try later: boolean isSatisfiedF2 = field2Builder.toString().equals("0000000000000000000000000000000000000000");
                String f8 = field8Builder.toString().replace("0", "");
                String f9 = field9Builder.toString().replace("0", "");
                if(f3.isEmpty() && f8.isEmpty() && f9.isEmpty()) {
//                            log.info("Filtered out event [" + decodedEvent + "]");
                } else {
                    decodedEvents.add(decodedEvent);
                }
            }
            for(Event decodedEvent: decodedEvents) {
                count.incrementAndGet();
                double latency = (System.currentTimeMillis() - decodedEvent.getTimeStamp());
                latencyValuesLock.lock();
                heLatencyValuesLock.lock();
                latencyValues.add(latency);
                heLatencyValues.add(latency);
                heLatencyValuesLock.unlock();
                latencyValuesLock.unlock();
            }
        } catch (Throwable th) {
            log.error("Error occurred while decoding [" + th + "], Event [" + event + "]");
            th.printStackTrace();
        }
    }

    public void checkOutOfOrder(long timestamp){
        if (lastTimeStamp == 0){
            lastTimeStamp = timestamp;
        } else {
            if (timestamp < lastTimeStamp){
                outOfOrderEventCount++;
                System.out.println("Out of order event[Lsat timestamp=" + lastTimeStamp + " , thisTimeStamp="
                        + timestamp + ", Count=" + outOfOrderEventCount + ", Non count=" + nonOutOfOrderEventCount+ "]");
            } else {
                nonOutOfOrderEventCount++;
                lastTimeStamp = timestamp;
            }
        }
    }


    public void start(String host, int receiverPort, String protocol, String sampleNumber) throws DataBridgeException, StreamDefinitionStoreException {
        Util.setKeyStoreParams();
        DataBridge databridge = new DataBridge(new AuthenticationHandler() {
            public boolean authenticate(String userName,
                                        String password) {
                return true;// allays authenticate to true
            }

            public String getTenantDomain(String userName) {
                return "admin";
            }

            public int getTenantId(String s) throws UserStoreException {
                return -1234;
            }

            public void initContext(AgentSession agentSession) {

            }

            public void destroyContext(AgentSession agentSession) {

            }

            public void setThreadLocalContext(AgentSession agentSession) {
                //To change body of implemented methods use File | Settings | File Templates.
            }
        }, streamDefinitionStore, Util.getDataBridgeConfigPath());

        for (StreamDefinition streamDefinition : Util.loadStreamDefinitions(sampleNumber)) {
            streamDefinitionStore.saveStreamDefinitionToStore(streamDefinition, -1234);
            log.info("StreamDefinition of '" + streamDefinition.getStreamId() + "' added to store");
        }

        databridge.subscribe(new AgentCallback() {

            public void definedStream(StreamDefinition streamDefinition,
                                      int tenantID) {
                log.info("StreamDefinition " + streamDefinition);
            }

            public void removeStream(StreamDefinition streamDefinition, int tenantID) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            public void receive(List<Event> eventList, Credentials credentials) {
                if (isUsingKslack) {
                    kslack.addEvents(eventList);
                } else {
                    onReceive(eventList);
                }
            }

        });

        if (protocol.equalsIgnoreCase("binary")) {
            binaryDataReceiver = new BinaryDataReceiver(new BinaryDataReceiverConfiguration(receiverPort + 100, receiverPort), databridge);
            try {
                binaryDataReceiver.start();
            } catch (IOException e) {
                log.error("Error occurred when reading the file : " + e.getMessage(), e);
            }
        } else {
            thriftDataReceiver = new ThriftDataReceiver(receiverPort + 100, receiverPort, databridge);
            thriftDataReceiver.start(host);
        }

        homomorphicEncDecService = new HomomorphicEncDecService();
        homomorphicEncDecService.init(StatisticsCollector.prop.getProperty("key.file.path"));

        decodingWorkers = Executors.newFixedThreadPool(20, new ThreadFactoryBuilder().setNameFormat("Composite-Event-Decode-Workers").build());

        log.info("Test Server Started");
    }


    public void stop() {
        if (thriftDataReceiver != null) {
            thriftDataReceiver.stop();
        }
        if (binaryDataReceiver != null) {
            binaryDataReceiver.stop();
        }
        log.info("Test Server Stopped");
    }
}
