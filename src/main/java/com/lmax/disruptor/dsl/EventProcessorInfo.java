/*
 * Copyright 2011 LMAX Ltd.
 *
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
package com.lmax.disruptor.dsl;

import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

import java.util.concurrent.ThreadFactory;

/**
 * Wrapper class to tie together a particular event processing stage</p>
 *
 * 包装类，把一个事件处理阶段相关的组件关联在一起。
 *
 * <p><p>Tracks the event processor instance, the event handler instance, and sequence barrier which the stage is attached to.</p>
 *
 * 跟踪该阶段所关联的事件处理器实例、事件处理器（handler）实例以及序号屏障。
 *
 */
class EventProcessorInfo implements ConsumerInfo
{
    private final EventProcessor eventprocessor;
    private final SequenceBarrier barrier;
    private boolean endOfChain = true;

    /**
     * Creates the info for one event processor and its barrier.
     *
     * 为一个事件处理器及其屏障创建信息对象。
     */
    EventProcessorInfo(final EventProcessor eventprocessor, final SequenceBarrier barrier)
    {
        this.eventprocessor = eventprocessor;
        this.barrier = barrier;
    }

    /**
     * Returns the underlying event processor.
     *
     * 返回底层的事件处理器。
     */
    public EventProcessor getEventProcessor()
    {
        return eventprocessor;
    }

    /**
     * Returns the sequence this processor has reached.
     *
     * 返回该处理器当前处理到的序号。
     */
    @Override
    public Sequence[] getSequences()
    {
        return new Sequence[]{eventprocessor.getSequence()};
    }

    /**
     * Returns the barrier this stage waits on.
     *
     * 返回该阶段等待的序号屏障。
     */
    @Override
    public SequenceBarrier getBarrier()
    {
        return barrier;
    }

    /**
     * Indicates whether this stage is the end of the dependency chain.
     *
     * 指示该阶段是否是依赖链的末端。
     */
    @Override
    public boolean isEndOfChain()
    {
        return endOfChain;
    }

    /**
     * Starts the event processor on a new thread from the supplied factory.
     *
     * 用给定的线程工厂创建新线程并启动事件处理器。
     */
    @Override
    public void start(final ThreadFactory threadFactory)
    {
        final Thread thread = threadFactory.newThread(eventprocessor);
        if (null == thread)
        {
            throw new RuntimeException("Failed to create thread to run: " + eventprocessor);
        }

        thread.start();
    }

    /**
     * Stops the event processor.
     *
     * 停止事件处理器。
     */
    @Override
    public void halt()
    {
        eventprocessor.halt();
    }

    /**
     * Marks that this processor's sequence is gated on by another stage.
     *
     * 标记该处理器的序号已被其他阶段用作门控条件，因此它不再是依赖链末端。
     */
    @Override
    public void markAsUsedInBarrier()
    {
        endOfChain = false;
    }

    /**
     * Indicates whether the event processor is currently running.
     *
     * 指示事件处理器当前是否正在运行。
     */
    @Override
    public boolean isRunning()
    {
        return eventprocessor.isRunning();
    }
}
