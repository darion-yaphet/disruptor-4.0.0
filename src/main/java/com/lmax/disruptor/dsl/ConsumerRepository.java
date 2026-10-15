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

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventHandlerIdentity;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

/**
 * Provides a repository mechanism to associate {@link EventHandler}s with {@link EventProcessor}s
 *
 * 提供仓库机制，用于维护 EventHandler 与 EventProcessor 之间的关联关系。
 */
class ConsumerRepository
{
    private final Map<EventHandlerIdentity, EventProcessorInfo> eventProcessorInfoByEventHandler =
        new IdentityHashMap<>();
    private final Map<Sequence, ConsumerInfo> eventProcessorInfoBySequence =
        new IdentityHashMap<>();
    private final Collection<ConsumerInfo> consumerInfos = new ArrayList<>();

    /**
     * Registers an event processor together with its handler identity and barrier.
     *
     * 注册一个事件处理器及其对应的事件处理器标识和序号屏障。
     */
    public void add(
        final EventProcessor eventprocessor,
        final EventHandlerIdentity handlerIdentity,
        final SequenceBarrier barrier)
    {
        final EventProcessorInfo consumerInfo = new EventProcessorInfo(eventprocessor, barrier);
        eventProcessorInfoByEventHandler.put(handlerIdentity, consumerInfo);
        eventProcessorInfoBySequence.put(eventprocessor.getSequence(), consumerInfo);
        consumerInfos.add(consumerInfo);
    }

    /**
     * Registers an event processor that is not associated with a handler.
     *
     * 注册一个不与事件处理器（handler）关联的事件处理器。
     */
    public void add(final EventProcessor processor)
    {
        final EventProcessorInfo consumerInfo = new EventProcessorInfo(processor, null);
        eventProcessorInfoBySequence.put(processor.getSequence(), consumerInfo);
        consumerInfos.add(consumerInfo);
    }

    /**
     * Starts all registered consumers.
     *
     * 启动所有已注册的消费者。
     */
    public void startAll(final ThreadFactory threadFactory)
    {
        consumerInfos.forEach(c -> c.start(threadFactory));
    }

    /**
     * Halts all registered consumers.
     *
     * 停止所有已注册的消费者。
     */
    public void haltAll()
    {
        consumerInfos.forEach(ConsumerInfo::halt);
    }

    /**
     * Checks whether any end-of-chain consumer is still behind the cursor.
     *
     * 检查是否有依赖链末端的消费者仍落后于游标（即存在未处理的积压事件）。
     */
    public boolean hasBacklog(final long cursor, final boolean includeStopped)
    {
        // 只检查依赖链末端的消费者，逐个比较其序号与游标
        for (ConsumerInfo consumerInfo : consumerInfos)
        {
            if ((includeStopped || consumerInfo.isRunning()) && consumerInfo.isEndOfChain())
            {
                final Sequence[] sequences = consumerInfo.getSequences();
                for (Sequence sequence : sequences)
                {
                    if (cursor > sequence.get())
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     * Looks up the event processor for the given handler, throwing if none is registered.
     *
     * 查找给定事件处理器标识对应的事件处理器；未注册时抛出异常。
     */
    public EventProcessor getEventProcessorFor(final EventHandlerIdentity handlerIdentity)
    {
        final EventProcessorInfo eventprocessorInfo = getEventProcessorInfo(handlerIdentity);
        if (eventprocessorInfo == null)
        {
            throw new IllegalArgumentException("The event handler " + handlerIdentity + " is not processing events.");
        }

        return eventprocessorInfo.getEventProcessor();
    }

    /**
     * Returns the sequence of the processor for the given handler.
     *
     * 返回给定事件处理器标识对应处理器的序号。
     */
    public Sequence getSequenceFor(final EventHandlerIdentity handlerIdentity)
    {
        return getEventProcessorFor(handlerIdentity).getSequence();
    }

    /**
     * Marks the processors owning the given sequences as no longer end-of-chain.
     *
     * 将给定序号所属的处理器标记为"不再是依赖链末端"（因为有其他阶段以它们为门控）。
     */
    public void unMarkEventProcessorsAsEndOfChain(final Sequence... barrierEventProcessors)
    {
        for (Sequence barrierEventProcessor : barrierEventProcessors)
        {
            getEventProcessorInfo(barrierEventProcessor).markAsUsedInBarrier();
        }
    }

    /**
     * Returns the barrier associated with the given handler, or null if none.
     *
     * 返回与给定事件处理器标识关联的序号屏障；不存在时返回 null。
     */
    public SequenceBarrier getBarrierFor(final EventHandlerIdentity handlerIdentity)
    {
        final ConsumerInfo consumerInfo = getEventProcessorInfo(handlerIdentity);
        return consumerInfo != null ? consumerInfo.getBarrier() : null;
    }

    /**
     * Looks up consumer info by handler identity.
     *
     * 按事件处理器标识查找消费者信息。
     */
    private EventProcessorInfo getEventProcessorInfo(final EventHandlerIdentity handlerIdentity)
    {
        return eventProcessorInfoByEventHandler.get(handlerIdentity);
    }

    /**
     * Looks up consumer info by the processor's sequence.
     *
     * 按处理器序号查找消费者信息。
     */
    private ConsumerInfo getEventProcessorInfo(final Sequence barrierEventProcessor)
    {
        return eventProcessorInfoBySequence.get(barrierEventProcessor);
    }
}
