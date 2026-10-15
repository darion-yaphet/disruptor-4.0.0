package com.lmax.disruptor.dsl;

import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

import java.util.concurrent.ThreadFactory;

/**
 * Provides access to the state of a consumer registered with the Disruptor.
 *
 * 提供对已注册到 Disruptor 的消费者状态的访问。
 */
interface ConsumerInfo
{
    /**
     * Returns the sequences this consumer is tracking or writing.
     *
     * 返回该消费者正在跟踪或写入的序号。
     */
    Sequence[] getSequences();

    /**
     * Returns the barrier this consumer waits on.
     *
     * 返回该消费者等待的序号屏障。
     */
    SequenceBarrier getBarrier();

    /**
     * Indicates whether this consumer is the end of the dependency chain.
     *
     * 指示该消费者是否是依赖链的末端。
     */
    boolean isEndOfChain();

    /**
     * Starts the consumer using the supplied thread factory.
     *
     * 使用给定的线程工厂启动该消费者。
     */
    void start(ThreadFactory threadFactory);

    /**
     * Stops the consumer.
     *
     * 停止该消费者。
     */
    void halt();

    /**
     * Records that this consumer is referenced by a barrier.
     *
     * 记录该消费者已被某个屏障引用。
     */
    void markAsUsedInBarrier();

    /**
     * Indicates whether the consumer is currently running.
     *
     * 指示该消费者当前是否正在运行。
     */
    boolean isRunning();
}
