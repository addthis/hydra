/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.addthis.hydra.store.util;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import java.lang.management.ManagementFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

import com.addthis.basis.jmx.MapMBean;
import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Files;
import com.addthis.basis.util.Strings;



public class MeterFileLogger implements Runnable {

    private final DecimalFormat extFormat = new DecimalFormat("00000");
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yy/MM/dd-HH:mm:ss");
    private final boolean meterJMX = System.getProperty("meterFileLogger.jmx", "1").equals("1");

    private final MeterDataSource meter;
    private final File dirout;
    private final String basename;
    private final long logInterval;
    private final int rollLines;
    private final Thread thread;

    private AtomicBoolean done;
    private int lineCount = 0;
    private int currentExt = 0;
    private FileOutputStream currentOutput;

    private Map<String, Long> lastMap;
    private MapMBean mbean;
    private ObjectName beanName;

    public static interface MeterDataSource {

        public Map<String, Long> getIntervalData();
    }

    public MeterFileLogger(MeterDataSource meter, File dirout, String basename, long logInterval, int rollLines) throws IOException {
        this.meter = meter;
        this.dirout = dirout;
        this.basename = basename;
        this.logInterval = logInterval;
        this.rollLines = rollLines;
        this.done = new AtomicBoolean(false);

        Files.initDirectory(dirout);
        initNextOutput();

        thread = new Thread(this, "MeterFileLogger " + dirout);
        thread.setDaemon(true);
        thread.start();
    }

    private synchronized void initNextOutput() throws IOException {
        if (currentOutput != null) {
            currentOutput.flush();
            currentOutput.close();
        }
        while (!done.get()) {
            File next = new File(dirout, basename + "-" + extFormat.format(currentExt++) + ".csv");
            if (!next.exists()) {
                currentOutput = new FileOutputStream(next);
                lineCount = 0;
                break;
            }
        }
    }

    public void terminate() {
        if (done.compareAndSet(false, true)) {
            thread.interrupt();
            try {
                thread.join();
                initNextOutput();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override public void run() {
        if (meterJMX) {
            startBean();
        }

        while (!done.get()) {
            try {
                Thread.sleep(logInterval);
                Map<String, Long> map = meter.getIntervalData();
                if (meterJMX) {
                    lastMap = new HashMap<>(map);
                    mbean.setMap(lastMap);
                }
                if (lineCount++ == 0) {
                    LinkedList<String> line = new LinkedList<>();
                    line.add("\"date\"");
                    for (String label : map.keySet()) {
                        line.add("\"" + label + "\"");
                    }
                    currentOutput.write(Bytes.toBytes(Strings.join(line.toArray(), ",") + "\n"));
                }
                LinkedList<String> line = new LinkedList<>();
                line.add(dateFormat.format(System.currentTimeMillis()));
                for (Long val : map.values()) {
                    line.add(Long.toString(val));
                }
                currentOutput.write(Bytes.toBytes(Strings.join(line.toArray(), ",") + "\n"));
                currentOutput.flush();
            } catch (InterruptedException e) {
                if (!done.get()) {
                    e.printStackTrace();
                }
                continue;
            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }
        }

        if (meterJMX) {
            stopBean();
        }
    }

    protected void startBean() {
        mbean = new MapMBean(lastMap);
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            beanName = new ObjectName(this.getClass().getName() + ":type=" +
                                      genBeanSuffix());
            mbs.registerMBean(mbean, beanName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void stopBean() {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            if (beanName != null) {
                mbs.unregisterMBean(beanName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected String genBeanSuffix() {
        String[] path = dirout.getAbsolutePath().split("minion");
        int len = path.length;
        String basePath;
        if (len == 1) {
            basePath = path[0];
        } else if (len >= 2) {
            basePath = path[1];
        } else {
            basePath = dirout.getPath();
        }
        String[] className = meter.getClass().getName().split("\\.");
        return className[className.length - 1] + basePath; //: todo shortname?
    }
}
