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
    private static final int maxEmailLength = 40;
    private static final int compositeEventSize = 10;

    public static HomomorphicEncDecService homomorphicEncDecService;
    private static ExecutorService decodingWorkers;

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
            String[] field1Arr = field1.split(FIELD_SEPARATOR);
            String field5 = (String) payloadData[4];
            String[] field5Arr = field5.split(FIELD_SEPARATOR);
            String field6 = (String) payloadData[5];
            String[] field6Arr = field6.split(FIELD_SEPARATOR);
            String field7 = (String) payloadData[6];
            String[] field7Arr = field7.split(FIELD_SEPARATOR);

            String field2 = (String) payloadData[1];
            String decryptedField2 = homomorphicEncDecService.decryptLongVector(field2);
            String[] decryptedField2Arr = decryptedField2.split(COMMA_SEPARATOR);

            String field3 = (String) payloadData[2];
            String decryptedField3 = homomorphicEncDecService.decryptLongVector(field3);
            String[] decryptedField3Arr = decryptedField3.split(COMMA_SEPARATOR);

            String field4 = (String) payloadData[3];
            String decryptedField4 = homomorphicEncDecService.decryptLongVector(field4);
            String[] decryptedField4Arr = decryptedField4.split(COMMA_SEPARATOR);

            for(int i = 0; i < compositeEventSize; i++) {
                StringBuilder field2Builder = new StringBuilder();
                StringBuilder field3Builder = new StringBuilder();
                StringBuilder field4Builder = new StringBuilder();
                for(int j = 0; j < maxEmailLength; j++) {
                    field2Builder.append(decryptedField2Arr[(i * maxEmailLength) + j]);
                    field3Builder.append(decryptedField3Arr[(i * maxEmailLength) + j]);
                    field4Builder.append(decryptedField4Arr[(i * maxEmailLength) + j]);
                }

                String f5Val = (field5Arr.length > i) ? field5Arr[i] : "";
                String f6Val = (field6Arr.length > i) ? field6Arr[i] : "";
                String f7Val = (field7Arr.length > i) ? field7Arr[i] : "";

                Object[] payloadDataArray = {
                        Long.valueOf(field1Arr[i]),
                        field2Builder.toString(),
                        field3Builder.toString(),
                        field4Builder.toString(),
                        f5Val,
                        f6Val,
                        f7Val
                };
                Event decodedEvent = new Event(event.getStreamId(), Long.valueOf(field1Arr[i]), null, null, payloadDataArray);
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

        decodingWorkers = Executors.newFixedThreadPool(100);

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
