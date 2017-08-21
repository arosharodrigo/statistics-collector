package research.schedular;

/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.databridge.commons.StreamDefinition;
import org.wso2.carbon.databridge.commons.exception.MalformedStreamDefinitionException;
import org.wso2.carbon.databridge.commons.utils.EventDefinitionConverterUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Util {
    private static Log log = LogFactory.getLog(Util.class);

    static String baseFilePath =   "/home/arosha/projects/Siddhi/projects/start-2017-06-19/Homomorphic/projects/my-git/statistics-collector/";
    static String pseudoCarbonHome = "/home/arosha/projects/Siddhi/projects/start-2017-06-19/Homomorphic/projects/my-git/statistics-collector/src/main/java/files";
    static File configFiles = new File(baseFilePath + File.separator + "src/main/java/files/configs");
    static File streamDefinitionFile = new File(baseFilePath+ File.separator + "src/main/java/files/streamDefinitions");
    static PrintWriter writer = null;



    public static void initializeResultFile(String header) throws FileNotFoundException {
        writer = new PrintWriter(baseFilePath + File.separator +"results.csv");
        writer.println(header);
        writer.flush();
    }

    public static void writeResult(String data){
        writer.println(data);
        writer.flush();
    }

    public static void closeFile(){
        writer.close();
    }

    public static void setTrustStoreParams() {
        String trustStore = configFiles.getAbsolutePath();
        System.setProperty("javax.net.ssl.trustStore", trustStore + "" + File.separator + "client-truststore.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "wso2carbon");

    }

    // This is to set CARBON_HOME for data bridge to initialize. Otherwise it will fail to receive events though the server is started
    public static void setPseudoCarbonHome(){
        System.setProperty("carbon.home", pseudoCarbonHome);
    }

    public static void setKeyStoreParams() {
        String keyStore = configFiles.getAbsolutePath();
        System.setProperty("Security.KeyStore.Location", keyStore + "" + File.separator + "wso2carbon.jks");
        System.setProperty("Security.KeyStore.Password", "wso2carbon");

    }

    public static String getDataBridgeConfigPath() {
        return configFiles + File.separator + "data-bridge-config.xml";
    }

    public static List<StreamDefinition> loadStreamDefinitions(String sampleNumber) {
        File directory = streamDefinitionFile;
        List<StreamDefinition> streamDefinitions = new ArrayList<StreamDefinition>();
        if (!directory.exists()) {
            log.error("Cannot load stream definitions from " + directory.getAbsolutePath() + " directory not exist");
            return streamDefinitions;
        }
        if (!directory.isDirectory()) {
            log.error("Cannot load stream definitions from " + directory.getAbsolutePath() + " not a directory");
            return streamDefinitions;
        }
        File[] defFiles = directory.listFiles();

        if (defFiles != null) {
            for (final File fileEntry : defFiles) {
                if (!fileEntry.isDirectory()) {


                    BufferedReader bufferedReader = null;
                    StringBuilder stringBuilder = new StringBuilder();
                    try {
                        bufferedReader = new BufferedReader(new FileReader(fileEntry));
                        String line;
                        while ((line = bufferedReader.readLine()) != null) {
                            stringBuilder.append(line).append("\n");
                        }
                        StreamDefinition streamDefinition = EventDefinitionConverterUtils.convertFromJson(stringBuilder.toString().trim());
                        streamDefinitions.add(streamDefinition);
                    } catch (FileNotFoundException e) {
                        log.error("Error in reading file " + fileEntry.getName(), e);
                    } catch (IOException e) {
                        log.error("Error in reading file " + fileEntry.getName(), e);
                    } catch (MalformedStreamDefinitionException e) {
                        log.error("Error in converting Stream definition " + e.getMessage(), e);
                    } finally {
                        try {
                            if (bufferedReader != null) {
                                bufferedReader.close();
                            }
                        } catch (IOException e) {
                            log.error("Error occurred when reading the file : " + e.getMessage(), e);
                        }
                    }
                }
            }
        }

        return streamDefinitions;

    }


}