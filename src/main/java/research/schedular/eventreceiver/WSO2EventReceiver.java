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
import research.schedular.Util;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class WSO2EventReceiver {
    Logger log = Logger.getLogger(WSO2EventReceiver.class);
    ThriftDataReceiver thriftDataReceiver;
    BinaryDataReceiver binaryDataReceiver;
    AbstractStreamDefinitionStore streamDefinitionStore = new InMemoryStreamDefinitionStore();
    static final WSO2EventReceiver testServer = new WSO2EventReceiver();
    int totalCount = 0;
    AtomicInteger count = new AtomicInteger(0);
    final static double BATCH_SIZE = 10000.0;

    public static WSO2EventReceiver getInstance(){
        return testServer;
    }

    /*
    public static void main(String[] args) throws DataBridgeException, StreamDefinitionStoreException {
        testServer.start("0.0.0.0", 7661, "thrift", "");
        synchronized (testServer) {
            try {
                testServer.wait();
            } catch (InterruptedException ignored) {


            }
        }
    }*/

    public int getCount(){
        return count.get();
    }

    public void resetCount(){
        count.set(0);
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
                totalCount++;
                count.incrementAndGet();
                //if (totalCount % BATCH_SIZE == 0){
                //    log.info("Total Event Received : " + totalCount);
                //}
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
