package com.lmax.disruptor.dsl;

import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;

/**
 * A factory interface to make it possible to include custom event processors in a chain:
 *
 * 工厂接口，用于让自定义事件处理器能够加入处理器调用链：
 *
 * <pre><code>
 * disruptor.handleEventsWith(handler1).then((ringBuffer, barrierSequences) -&gt; new CustomEventProcessor(ringBuffer, barrierSequences));
 * </code></pre>
 *
 * @param <T> implementation storing the data for sharing during exchange or parallel coordination of an event.
 *
 * 参数 <T>：在事件交换或并行协调过程中存储共享数据的实现类型。
 */
public interface EventProcessorFactory<T>
{
    /**
     * Create a new event processor that gates on <code>barrierSequences</code>.
     *
     * 创建一个新的、以 barrierSequences 为门控条件的事件处理器。
     *
     * @param ringBuffer the ring buffer to receive events from.
     * @param barrierSequences the sequences to gate on
     * @return a new EventProcessor that gates on <code>barrierSequences</code> before processing events
     *
     * ringBuffer：从中接收事件的环形缓冲区；barrierSequences：作为门控条件的序号。
     * 返回一个新的、在处理事件前先等待 barrierSequences 的 EventProcessor。
     */
    EventProcessor createEventProcessor(RingBuffer<T> ringBuffer, Sequence[] barrierSequences);
}
