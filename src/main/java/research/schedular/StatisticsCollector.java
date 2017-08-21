package research.schedular;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.wso2.carbon.databridge.core.exception.DataBridgeException;
import org.wso2.carbon.databridge.core.exception.StreamDefinitionStoreException;
import research.schedular.eventreceiver.WSO2EventReceiver;

import java.io.FileNotFoundException;
import java.lang.management.ManagementFactory;
import java.text.DecimalFormat;
import java.util.Timer;
import java.util.TimerTask;

// mvn exec:java -Dexec.mainClass="research.schedular.StatisticsCollector"
public class StatisticsCollector {

    private static final int SAMPLE_RATE = 1000; // How often we read the statistics in milliseconds
    private static int sampleCount = 0;
    private static DecimalFormat decimalFormat = new DecimalFormat("#.00");
    private static final String RESULT_OUTPUT_FORMAT = "Time Elapsed(s), Load Average, Free Memory Percentage, Throughput(TPS), Avg. Latency(ms), Latency 95th Percentile";


    public static void main(String[] args) throws InterruptedException, DataBridgeException, StreamDefinitionStoreException, FileNotFoundException {

        // To avoid exception xml parsing error occur for java 8
        System.setProperty("org.xml.sax.driver", "com.sun.org.apache.xerces.internal.parsers.SAXParser");
        System.setProperty("javax.xml.parsers.DocumentBuilderFactory","com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
        System.setProperty("javax.xml.parsers.SAXParserFactory","com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");

        // Need to set a pseudo carbon home for data bridge to receive events. Otherwise it fails when trying to create the
        // privileged carbon context giving a NoClassDefFoundError.
        Util.setPseudoCarbonHome();

        new EventReceiverThread().start();

        Util.initializeResultFile(RESULT_OUTPUT_FORMAT);
        StatisticsCollector.startCollecting();
    }

    public static void startCollecting(){
        Timer statisticsCollectingTimer = new Timer();
        statisticsCollectingTimer.schedule(new StatisticsCollectorTask(), 0, SAMPLE_RATE);
    }

    /**
     * Thread to run WSO2 Event Receiver
     */

    static class EventReceiverThread extends Thread {
        static WSO2EventReceiver receiver = WSO2EventReceiver.getInstance();
        public void run() {
            synchronized (receiver){
                try {
                    receiver.start("0.0.0.0", 7661, "thrift", "");
                    receiver.wait();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        }


        public static int getReceivedEventCount(){
            return  receiver.getAndResetCount();
        }

        public static double[] getLatencyValues(){
            return receiver.getAndResetLatencyValues();
        }

    }

    /**
     * Timer task to collect statistics periodically
     */
    static class StatisticsCollectorTask extends TimerTask {
        com.sun.management.OperatingSystemMXBean operatingSystemMXBean = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();


        /**
         * The action to be performed by this timer task.
         */
        @Override
        public void run() {
            sampleCount++;
            int relievedEventCount = EventReceiverThread.getReceivedEventCount();
            double [] latencyValue = EventReceiverThread.getLatencyValues();

            StringBuilder result = new StringBuilder();
            result.append(sampleCount * SAMPLE_RATE/1000);// Time Elapsed
            result.append(",");
            result.append(operatingSystemMXBean.getSystemLoadAverage()); // Load Average
            result.append(",");
            result.append(decimalFormat.format(100.0 * (float)operatingSystemMXBean.getFreePhysicalMemorySize()/ (float)operatingSystemMXBean.getTotalPhysicalMemorySize())); // Free Memory Percentage
            result.append(",");
            result.append(1000.0 * (float) relievedEventCount/ (float)SAMPLE_RATE); // Throughput
            result.append(",");
            if (latencyValue.length == 0 || relievedEventCount == 0){
                result.append("0.0");// Latency
                result.append(",");
                result.append("0.0"); // 95th Percentile
            }else {
                result.append(decimalFormat.format(new Mean().evaluate(latencyValue, 0, latencyValue.length)));// Latency
                result.append(",");
                result.append(decimalFormat.format(new Percentile().evaluate(latencyValue, 95.0))); // 95th Percentile
            }

            System.out.println(RESULT_OUTPUT_FORMAT + " \n " + result.toString());

            Util.writeResult(result.toString());
        }
    }

}

