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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class WSO2EventReceiver {
    Logger log = Logger.getLogger(WSO2EventReceiver.class);
    ThriftDataReceiver thriftDataReceiver;
    BinaryDataReceiver binaryDataReceiver;
    AbstractStreamDefinitionStore streamDefinitionStore = new InMemoryStreamDefinitionStore();
    static final WSO2EventReceiver testServer = new WSO2EventReceiver();
    int totalCount = 0;
    AtomicInteger count = new AtomicInteger(0);
    List<Double> latencyValues = new ArrayList<Double>();

    Lock latencyValuesLock = new ReentrantLock();
    final static double BATCH_SIZE = 10000.0;

    SimpleKslack kslack = new SimpleKslack(this);
    private boolean isUsingKslack = false;

    private long lastTimeStamp = 0;
    private long outOfOrderEventCount = 0;
    private long nonOutOfOrderEventCount = 0;


    public static HomomorphicEncDecService homomorphicEncDecService;

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

    public void onReceive(List<Event> mixEventList){
        List<Event> eventList = new ArrayList<Event>();
        for (Event event : mixEventList) {
            if(event.getPayloadData().length > 2) {
                Event inEventComposite = event;
                Object[] payloadData = inEventComposite.getPayloadData();
                int eventSize = Integer.parseInt(String.valueOf(payloadData[2]));
                String encryptedResult = String.valueOf(payloadData[1]);
                String decryptedResult = homomorphicEncDecService.decryptLongVector(encryptedResult);

                String[] timestampArray = String.valueOf(payloadData[0]).split(",");
                String[] decryptedResultArray = decryptedResult.split(",");

//                Event[] decryptedEvents = new Event[eventSize];
                for(int i = 0;i < eventSize; i++) {
                    Event decryptedEvent = new Event("", inEventComposite.getTimeStamp(), null, null, new Object[]{timestampArray[i], decryptedResultArray[i]});
                    eventList.add(decryptedEvent);
                }
            } else {
                eventList.add(event);
            }
        }

        totalCount += eventList.size();
        count.addAndGet(eventList.size());
        latencyValuesLock.lock();
        for (Event event : eventList){
            latencyValues.add((double) (System.currentTimeMillis() - event.getTimeStamp()));
//            checkOutOfOrder(event.getTimeStamp());
        }

        latencyValuesLock.unlock();
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
            thriftDataReceiver = new ThriftDataReceiver(receiverPort, databridge);
            thriftDataReceiver.start(host);
        }

        homomorphicEncDecService = new HomomorphicEncDecService();
        homomorphicEncDecService.init(StatisticsCollector.prop.getProperty("key.file.path"));

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
