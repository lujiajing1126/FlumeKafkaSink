package org.apache.flume.sink.kafka;

/**
 * Created by megrez on 15/2/10.
 */
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class ConsumerTest implements Runnable {
    private KafkaStream<byte[], byte[]> m_stream;
    private int m_threadNumber;
    private CyclicBarrier cyclicBarrier;

    public ConsumerTest(KafkaStream<byte[],byte[]> a_stream, int a_threadNumber,CyclicBarrier cyclicBarrier) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
        this.cyclicBarrier = cyclicBarrier;
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext())
            System.out.println("Thread " + m_threadNumber + ": " + new String(it.next().message()));
        System.out.println("Shutting down Thread: " + m_threadNumber);
        try {
            cyclicBarrier.await();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
