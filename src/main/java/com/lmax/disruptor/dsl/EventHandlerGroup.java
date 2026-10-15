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

import com.lmax.disruptor.BatchRewindStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.RewindableEventHandler;
import com.lmax.disruptor.RewindableException;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

import java.util.Arrays;

/**
 * A group of {@link EventProcessor}s used as part of the {@link Disruptor}.
 *
 * 一组作为 Disruptor 组成部分的 EventProcessor，用于在 DSL 中表达消费者依赖关系。
 *
 * @param <T> the type of entry used by the event processors.
 *
 * 参数 <T>：事件处理器所使用条目的类型。
 */
public class EventHandlerGroup<T>
{
    private final Disruptor<T> disruptor;
    private final ConsumerRepository consumerRepository;
    private final Sequence[] sequences;

    /**
     * Creates a group over the given sequences belonging to the given disruptor.
     *
     * 基于给定的 Disruptor 和一组序号创建处理器分组。
     */
    EventHandlerGroup(
        final Disruptor<T> disruptor,
        final ConsumerRepository consumerRepository,
        final Sequence[] sequences)
    {
        this.disruptor = disruptor;
        this.consumerRepository = consumerRepository;
        this.sequences = Arrays.copyOf(sequences, sequences.length);
    }

    /**
     * Create a new event handler group that combines the consumers in this group with <code>otherHandlerGroup</code>.
     *
     * 创建一个新的事件处理器分组，把本组中的消费者与 otherHandlerGroup 合并在一起。
     *
     * @param otherHandlerGroup the event handler group to combine.
     * @return a new EventHandlerGroup combining the existing and new consumers into a single dependency group.
     *
     * otherHandlerGroup：要合并的事件处理器分组；返回把已有消费者与新消费者合并为同一个依赖组的新 EventHandlerGroup。
     */
    public EventHandlerGroup<T> and(final EventHandlerGroup<T> otherHandlerGroup)
    {
        final Sequence[] combinedSequences = new Sequence[this.sequences.length + otherHandlerGroup.sequences.length];
        System.arraycopy(this.sequences, 0, combinedSequences, 0, this.sequences.length);
        System.arraycopy(
            otherHandlerGroup.sequences, 0,
            combinedSequences, this.sequences.length, otherHandlerGroup.sequences.length);
        return new EventHandlerGroup<>(disruptor, consumerRepository, combinedSequences);
    }

    /**
     * Create a new event handler group that combines the handlers in this group with <code>processors</code>.
     *
     * 创建一个新的事件处理器分组，把本组中的处理器与 processors 合并在一起。
     *
     * @param processors the processors to combine.
     * @return a new EventHandlerGroup combining the existing and new processors into a single dependency group.
     *
     * processors：要合并的处理器；返回把已有处理器与新处理器合并为同一个依赖组的新 EventHandlerGroup。
     */
    public EventHandlerGroup<T> and(final EventProcessor... processors)
    {
        Sequence[] combinedSequences = new Sequence[sequences.length + processors.length];

        for (int i = 0; i < processors.length; i++)
        {
            consumerRepository.add(processors[i]);
            combinedSequences[i] = processors[i].getSequence();
        }
        System.arraycopy(sequences, 0, combinedSequences, processors.length, sequences.length);

        return new EventHandlerGroup<>(disruptor, consumerRepository, combinedSequences);
    }

    /**
     * <p>Set up batch handlers to consume events from the ring buffer. These handlers will only process events
     * after every {@link EventProcessor} in this group has processed the event.</p>
     *
     * 设置批处理器来消费环形缓冲区中的事件。这些处理器只会在本组中每个 EventProcessor
     * 都处理完该事件之后才开始处理它。
     *
     * <p>This method is generally used as part of a chain. For example if the handler <code>A</code> must
     * process events before handler <code>B</code>:</p>
     *
     * 本方法通常作为调用链的一环使用。例如处理器 A 必须先于处理器 B 处理事件：
     *
     * <pre><code>dw.handleEventsWith(A).then(B);</code></pre>
     *
     * @param handlers the batch handlers that will process events.
     * @return a {@link EventHandlerGroup} that can be used to set up a event processor barrier over the created event processors.
     *
     * handlers：将处理事件的批处理器；返回一个 EventHandlerGroup，可用来在创建的事件处理器之上设置屏障。
     */
    @SafeVarargs
    public final EventHandlerGroup<T> then(final EventHandler<? super T>... handlers)
    {
        return handleEventsWith(handlers);
    }

    /**
     * <p>Set up batch handlers to consume events from the ring buffer. These handlers will only process events
     * after every {@link EventProcessor} in this group has processed the event.</p>
     *
     * 设置批处理器来消费环形缓冲区中的事件。这些处理器只会在本组中每个 EventProcessor
     * 都处理完该事件之后才开始处理它。
     *
     * <p>This method is generally used as part of a chain. For example if the handler <code>A</code> must
     * process events before handler <code>B</code>:</p>
     *
     * 本方法通常作为调用链的一环使用。例如处理器 A 必须先于处理器 B 处理事件：
     *
     * <pre><code>dw.handleEventsWith(A).then(B);</code></pre>
     *
     * @param batchRewindStrategy a {@link BatchRewindStrategy} for customizing how to handle a {@link RewindableException}.
     * @param handlers            the rewindable event handlers that will process events.
     * @return a {@link EventHandlerGroup} that can be used to set up a event processor barrier over the created event processors.
     *
     * batchRewindStrategy：用于自定义如何处理 RewindableException 的 BatchRewindStrategy；
     * handlers：将处理事件的可回绕事件处理器；
     * 返回一个 EventHandlerGroup，可用来在创建的事件处理器之上设置屏障。
     */
    @SafeVarargs
    public final EventHandlerGroup<T> then(final BatchRewindStrategy batchRewindStrategy,
                                           final RewindableEventHandler<? super T>... handlers)
    {
        return handleEventsWith(batchRewindStrategy, handlers);
    }

    /**
     * <p>Set up custom event processors to handle events from the ring buffer. The Disruptor will
     * automatically start these processors when {@link Disruptor#start()} is called.</p>
     *
     * 设置自定义事件处理器来处理环形缓冲区中的事件。调用 Disruptor#start() 时，
     * Disruptor 会自动启动这些处理器。
     *
     * <p>This method is generally used as part of a chain. For example if the handler <code>A</code> must
     * process events before handler <code>B</code>:</p>
     *
     * 本方法通常作为调用链的一环使用。例如处理器 A 必须先于处理器 B 处理事件：
     *
     * @param eventProcessorFactories the event processor factories to use to create the event processors that will process events.
     * @return a {@link EventHandlerGroup} that can be used to chain dependencies.
     *
     * eventProcessorFactories：用于创建事件处理器的事件处理器工厂；
     * 返回一个可用于串联依赖关系的 EventHandlerGroup。
     */
    @SafeVarargs
    public final EventHandlerGroup<T> then(final EventProcessorFactory<T>... eventProcessorFactories)
    {
        return handleEventsWith(eventProcessorFactories);
    }

    /**
     * <p>Set up batch handlers to handle events from the ring buffer. These handlers will only process events
     * after every {@link EventProcessor} in this group has processed the event.</p>
     *
     * 设置批处理器来处理环形缓冲区中的事件。这些处理器只会在本组中每个 EventProcessor
     * 都处理完该事件之后才开始处理它。
     *
     * <p>This method is generally used as part of a chain. For example if <code>A</code> must
     * process events before <code>B</code>:</p>
     *
     * 本方法通常作为调用链的一环使用。例如 A 必须先于 B 处理事件：
     *
     * <pre><code>dw.after(A).handleEventsWith(B);</code></pre>
     *
     * @param handlers the batch handlers that will process events.
     * @return a {@link EventHandlerGroup} that can be used to set up a event processor barrier over the created event processors.
     *
     * handlers：将处理事件的批处理器；返回一个 EventHandlerGroup，可用来在创建的事件处理器之上设置屏障。
     */
    @SafeVarargs
    public final EventHandlerGroup<T> handleEventsWith(final EventHandler<? super T>... handlers)
    {
        return disruptor.createEventProcessors(sequences, handlers);
    }

    /**
     * <p>Set up batch handlers to handle events from the ring buffer. These handlers will only process events
     * after every {@link EventProcessor} in this group has processed the event.</p>
     *
     * 设置批处理器来处理环形缓冲区中的事件。这些处理器只会在本组中每个 EventProcessor
     * 都处理完该事件之后才开始处理它。
     *
     * <p>This method is generally used as part of a chain. For example if <code>A</code> must
     * process events before <code>B</code>:</p>
     *
     * 本方法通常作为调用链的一环使用。例如 A 必须先于 B 处理事件：
     *
     * <pre><code>dw.after(A).handleEventsWith(B);</code></pre>
     *
     * @param batchRewindStrategy a {@link BatchRewindStrategy} for customizing how to handle a {@link RewindableException}.
     * @param handlers            the rewindable event handlers that will process events.
     * @return a {@link EventHandlerGroup} that can be used to set up a event processor barrier over the created event processors.
     *
     * batchRewindStrategy：用于自定义如何处理 RewindableException 的 BatchRewindStrategy；
     * handlers：将处理事件的可回绕事件处理器；
     * 返回一个 EventHandlerGroup，可用来在创建的事件处理器之上设置屏障。
     */
    @SafeVarargs
    public final EventHandlerGroup<T> handleEventsWith(final BatchRewindStrategy batchRewindStrategy,
                                                       final RewindableEventHandler<? super T>... handlers)
    {
        return disruptor.createEventProcessors(sequences, batchRewindStrategy, handlers);
    }

    /**
     * <p>Set up custom event processors to handle events from the ring buffer. The Disruptor will
     * automatically start these processors when {@link Disruptor#start()} is called.</p>
     *
     * 设置自定义事件处理器来处理环形缓冲区中的事件。调用 Disruptor#start() 时，
     * Disruptor 会自动启动这些处理器。
     *
     * <p>This method is generally used as part of a chain. For example if <code>A</code> must
     * process events before <code>B</code>:</p>
     *
     * 本方法通常作为调用链的一环使用。例如 A 必须先于 B 处理事件：
     *
     * <pre><code>dw.after(A).handleEventsWith(B);</code></pre>
     *
     * @param eventProcessorFactories the event processor factories to use to create the event processors that will process events.
     * @return a {@link EventHandlerGroup} that can be used to chain dependencies.
     *
     * eventProcessorFactories：用于创建事件处理器的事件处理器工厂；
     * 返回一个可用于串联依赖关系的 EventHandlerGroup。
     */
    @SafeVarargs
    public final EventHandlerGroup<T> handleEventsWith(final EventProcessorFactory<T>... eventProcessorFactories)
    {
        return disruptor.createEventProcessors(sequences, eventProcessorFactories);
    }

    /**
     * Create a dependency barrier for the processors in this group.
     * This allows custom event processors to have dependencies on
     * {@link com.lmax.disruptor.BatchEventProcessor}s created by the disruptor.
     *
     * 为本组中的处理器创建一个依赖屏障，使自定义事件处理器能够依赖于
     * Disruptor 创建的 BatchEventProcessor。
     *
     * @return a {@link SequenceBarrier} including all the processors in this group.
     *
     * 返回一个包含本组所有处理器的 SequenceBarrier。
     */
    public SequenceBarrier asSequenceBarrier()
    {
        return disruptor.getRingBuffer().newBarrier(sequences);
    }
}
