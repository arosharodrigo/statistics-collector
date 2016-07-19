package research.schedular;

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

    private static final int SAMPLE_RATE = 10000; // How often we read the statistics in milliseconds
    private static int sampleCount = 0;
    private static DecimalFormat decimalFormat = new DecimalFormat("#.00");
    private static final String RESULT_OUTPUT_FORMAT = "Time Elapsed(s), Load Average, Free Memory Percentage, Throughput(TPS)";


    public static void main(String[] args) throws InterruptedException, DataBridgeException, StreamDefinitionStoreException, FileNotFoundException {
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
            return  receiver.getCount();
        }

        public static void resetReceivedEventCount(){
            receiver.resetCount();
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

            StringBuilder result = new StringBuilder();
            result.append(sampleCount * SAMPLE_RATE/1000);
            result.append(",");
            result.append(operatingSystemMXBean.getSystemLoadAverage());
            result.append(",");
            result.append(decimalFormat.format(100.0 * (float)operatingSystemMXBean.getFreePhysicalMemorySize()/ (float)operatingSystemMXBean.getTotalPhysicalMemorySize()));
            result.append(",");
            result.append(1000.0 * (float)EventReceiverThread.getReceivedEventCount() / (float)SAMPLE_RATE);

            System.out.println(RESULT_OUTPUT_FORMAT + " : " + result.toString());
            Util.writeResult(result.toString());

            EventReceiverThread.resetReceivedEventCount();
        }
    }

}

