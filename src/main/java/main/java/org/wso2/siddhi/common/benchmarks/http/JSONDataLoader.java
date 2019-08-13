/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package main.java.org.wso2.siddhi.common.benchmarks.http;

import com.google.common.base.Splitter;
import org.apache.kafka.clients.producer.Producer;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.joda.time.format.ISODateTimeFormat;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.*;
import java.nio.charset.Charset;

import java.util.*;

import java.util.concurrent.locks.ReentrantLock;


/**
 * Data loader for EDGAR log files.
 */

public class JSONDataLoader extends Thread {
    private long events = 0;
    private long startTime;
    private Splitter splitter = Splitter.on(',');
    private InputHandler inputHandler;
    long startTime2;
    long dataStartTime;
    int count;
    int randTime=0;
    private int temp = 0;
    private long byteCount = 0L;
    JSONDataLoader jsLoader = null;
    static ReentrantLock lock = new ReentrantLock();
    public static BufferedWriter bw;

    private static final Logger log = Logger.getLogger(JSONDataLoader.class);

    public static void main(String[] args) {

        BasicConfigurator.configure();

        log.info("Welcome to kafka message sender");
        
        try {
            File file = new File("/home/madushi/FYP/LogProcessor/log-processor/datarate.csv");

            BufferedWriter bw = null;
            FileWriter fw = null;
            if (!file.exists()) {
                file.getParentFile().mkdirs();
                file.createNewFile();
            }
            fw = new FileWriter(file.getAbsoluteFile(), true);
            bw = new BufferedWriter(fw);

            bw.write("Average Time stamp, " +
                    "Data Rate");

            bw.write("\n");
            bw.flush();
            bw.close();

        } catch (Exception e) {
            System.out.println("Error when writing to the file");
        }

        JSONDataLoader loader1 = new JSONDataLoader(bw);

        loader1.start();
    }

    public JSONDataLoader(BufferedWriter bw) {
        jsLoader = this;
        this.bw = bw;
    }

    public JSONDataLoader(JSONDataLoader js) {
        jsLoader = js;
    }

    public void incrementCommon(BufferedWriter bw, String jsonMessage) throws InterruptedException {
        jsLoader.temp++;
        try {
            jsLoader.byteCount+=jsonMessage.getBytes("UTF-8").length;
            log.info("Byte Count +++++++++++++++++++++++++++++++++++++"+byteCount);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        if (jsLoader.temp == 1) {
                jsLoader.startTime2 = System.currentTimeMillis();
                jsLoader.dataStartTime = jsLoader.startTime2;
                jsLoader.count =0;
            }
            long diff = System.currentTimeMillis() - jsLoader.startTime2;
            log.info("Sleep time = " +randTime);
            Thread.currentThread().sleep(randTime);
            if (diff != 0){

                log.info(Thread.currentThread().getName() + " spent : "
                        + diff + " for the event count : " + jsLoader.temp
                        + " with the  Data rate : " + (jsLoader.byteCount * 1000  / diff)+ " bytes per second and Time taken "+diff);
                try {
                    if((System.currentTimeMillis()-dataStartTime)> 10000){
                        log.info("dif="+diff);
                        log.info("byte count="+byteCount);
                        bw.write(jsLoader.count + "," + (jsLoader.byteCount * 1000 / diff) + "\n");
                        bw.flush();
                        jsLoader.dataStartTime = System.currentTimeMillis();
                        int rand = new Random().nextInt(4);

                        if(rand == 0 ){
                            int sleeptime = new Random().nextInt(1000);
                            if (sleeptime<500){
                                sleeptime = 500;
                            }
                            randTime = new Random().nextInt((int) byteCount/sleeptime);
                        }else {
                            randTime = 0;
                        }
                        jsLoader.count+=1;

                    }

                } catch(Exception e){
                    e.printStackTrace();
                    System.out.println("Exception occured while writing to the file");
                }
            }

    }

    public JSONDataLoader(InputHandler inputHandler) {
        super("Data Loader");
        this.inputHandler = inputHandler;
    }



    public void run() {
        BufferedReader br = null;
        BufferedWriter bw = null;
        try{

        File file = new File("/home/madushi/FYP/LogProcessor/log-processor/datarate.csv");
        FileWriter fw = null;
        if (!file.exists()) {
            file.getParentFile().mkdirs();
            file.createNewFile();
        }
        fw = new FileWriter(file.getAbsoluteFile(), true);
        bw = new BufferedWriter(fw);} catch (Exception e){
            System.out.println("Error reading");
        }

        ArrayList<Integer> list = new ArrayList<Integer>();

        list.add(0);
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);
        list.add(5);
        list.add(6);
        list.add(7);
        list.add(8);

        try {
	        Producer<String, String> producer = main.java.org.wso2.siddhi.common.benchmarks.http.KafkaMessageSender.createProducer();
            Locale locale = new Locale("en", "US");
            ResourceBundle bundle2 = ResourceBundle.getBundle("config", locale);

            String inputFilePath = bundle2.getString("input");
            br = new BufferedReader(new InputStreamReader(new FileInputStream(inputFilePath),
                    Charset.forName("UTF-8")));
            String line = br.readLine();
            line = br.readLine(); //We need to ignore the first line which has the headers.

            startTime = System.currentTimeMillis();

            int i = 0;
            int partitionCount =1;
            while (line != null) {
                events++;


                //We make an assumption here that we do not get empty strings due to missing values that may present
                // in the input data set.
                Iterator<String> dataStrIterator = splitter.split(line).iterator();
                String ipAddress = dataStrIterator.next(); //This variable provides the first three octets of the IP
                // address with the fourth octet obfuscated with a 3 character string that preserves the uniqueness of
                // the last octet without revealing the full identity of the IP (###.###.###.xxx)
                String date = dataStrIterator.next(); //yyyy-mm-dd
                String time = dataStrIterator.next(); //hh:mm:ss
                String zone = dataStrIterator.next(); //Zone is Apache log file zone. The time zone associated with the
                // server that completed processing the request.
                String cik = dataStrIterator.next(); //SEC Central Index Key (CIK) associated with the document
                // requested
                String accession = dataStrIterator.next(); //SEC document accession number associated with the
                // document requested
                String doc = dataStrIterator.next(); //This variable provides the filename of the file requested
                // including the document extension
                String code = dataStrIterator.next(); //Apache log file status code for the request
                String size = dataStrIterator.next(); //document file size
                size = size.equals("") ? "0.0" : size;


                String idx = dataStrIterator.next(); //takes on a value of 1 if the requester landed on the index page
                // of a set of documents (e.g., index.htm), and zero otherwise
                String norefer = dataStrIterator.next(); //takes on a value of one if the Apache log file referrer
                // field is empty, and zero otherwise
                String noagent = dataStrIterator.next(); //takes on a value of one if the Apache log file user agent
                // field is empty, and zero otherwise
                String find = dataStrIterator.next(); //numeric values from 0 to 10, that correspond to whether the
                // following character strings /[$string]/were found in the referrer field – this could indicate how
                // the document requester arrived at the document link (e.g., internal EDGAR search)
                String crawler = dataStrIterator.next(); //This variable takes on a value of one if the user agent
                // self-identifies as one of the following webcrawlers or has a user code of 404.
                String browser = dataStrIterator.next(); //This variable is a three character string that identifies
                // potential browser type by analyzing whether
                // the user agent field contained the following /[text]/
                //browser = browser.equals("") ? "-":browser;

                long timestamp = ISODateTimeFormat.dateTime().parseDateTime(date + "T" + time + ".000+0000")
                        .getMillis();

                log.info("Current time of " + Thread.currentThread().getName()
                        + " is " + System.currentTimeMillis());

                StringBuilder jsonDataItem = new StringBuilder();
                jsonDataItem.append("{ \"event\": { ");
                jsonDataItem.append("\"iij_timestamp\"");
                jsonDataItem.append(":");
                jsonDataItem.append(System.currentTimeMillis());
                jsonDataItem.append(",");

                jsonDataItem.append("\"ip\"");
                jsonDataItem.append(":\"");
                jsonDataItem.append(ipAddress);
                jsonDataItem.append("\",");

                jsonDataItem.append("\"timestamp\"");
                jsonDataItem.append(":");
                jsonDataItem.append(timestamp);
                jsonDataItem.append(",");

                jsonDataItem.append("\"zone\"");
                jsonDataItem.append(":");
                jsonDataItem.append(zone);
                jsonDataItem.append(",");

                jsonDataItem.append("\"cik\"");
                jsonDataItem.append(":");
                jsonDataItem.append(cik);
                jsonDataItem.append(",");

                jsonDataItem.append("\"accession\"");
                jsonDataItem.append(":\"");
                jsonDataItem.append(accession);
                jsonDataItem.append("\",");

                jsonDataItem.append("\"doc\"");
                jsonDataItem.append(":\"");
                jsonDataItem.append(doc);
                jsonDataItem.append("\",");

                jsonDataItem.append("\"code\"");
                jsonDataItem.append(":");
                jsonDataItem.append(code);
                jsonDataItem.append(",");

                jsonDataItem.append("\"size\"");
                jsonDataItem.append(":");
                jsonDataItem.append(size);
                jsonDataItem.append(",");

                jsonDataItem.append("\"idx\"");
                jsonDataItem.append(":");
                jsonDataItem.append(idx);
                jsonDataItem.append(",");

                jsonDataItem.append("\"norefer\"");
                jsonDataItem.append(":");
                jsonDataItem.append(norefer);
                jsonDataItem.append(",");

                jsonDataItem.append("\"noagent\"");
                jsonDataItem.append(":");
                jsonDataItem.append(noagent);
                jsonDataItem.append(",");

                jsonDataItem.append("\"find\"");
                jsonDataItem.append(":");
                jsonDataItem.append(find);
                jsonDataItem.append(",");

                jsonDataItem.append("\"crawler\"");
                jsonDataItem.append(":");
                jsonDataItem.append(crawler);
                jsonDataItem.append(",");


                jsonDataItem.append("\"groupID\"");
                jsonDataItem.append(":");
                jsonDataItem.append(list.get(i));
                jsonDataItem.append(",");

                if (i == 8) {
                    i = -1;
                }
                i++;


                jsonDataItem.append("\"browser\"");
                jsonDataItem.append(":\"");
                jsonDataItem.append(browser);
//                jsonDataItem.append(",");
                jsonDataItem.append("\" } }");

//                jsonDataItem.append("\"partitionNo\"");
//                jsonDataItem.append(":\"");
//                jsonDataItem.append(partitionCount);
//                jsonDataItem.append("\" } }");
//
//
//                if (partitionCount == 12) {
//                    partitionCount = 1;
//                }
//                partitionCount++;

                try {
                    main.java.org.wso2.siddhi.common.benchmarks.http.KafkaMessageSender.runProducer(jsonDataItem.toString(),producer);
                    log.info("Message sent to kafaka by "
                            + Thread.currentThread().getName());

                    incrementCommon(bw,jsonDataItem.toString());



                    try {
                        Thread.currentThread().sleep(10); //increase upto 500
                    } catch (InterruptedException e) {
                        log.info("Error: " + e.getMessage());
                    }

                } catch (InterruptedException e) {
                    log.error("Error sending an event to Input Handler, " + e.getMessage(), e);
                } catch (Exception e) {
                    log.error("Error: " + e.getMessage(), e);
                }

                line = br.readLine();
            }
        } catch (FileNotFoundException e) {
            log.error("Error in accessing the input file. " + e.getMessage(), e);
        } catch (IOException e2) {
            log.error("Error in accessing the input file. " + e2.getMessage(), e2);
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    log.error("Error in accessing the input file. " + e.getMessage(), e);
                }
            }
        }
    }
}
