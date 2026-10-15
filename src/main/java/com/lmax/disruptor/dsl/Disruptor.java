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

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BatchEventProcessorBuilder;
import com.lmax.disruptor.BatchRewindStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventHandlerIdentity;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RewindableEventHandler;
import com.lmax.disruptor.RewindableException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.util.Util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A DSL-style API for setting up the disruptor pattern around a ring buffer
 * (aka the Builder pattern).
 *
 * 用于围绕环形缓冲区搭建 disruptor 模式的 DSL 风格 API（即 Builder 模式）。
 *
 * <p>A simple example of setting up the disruptor with two event handlers that
 * must process events in order:
 *
 * <pre>
 * <code>Disruptor&lt;MyEvent&gt; disruptor = new Disruptor&lt;MyEvent&gt;(MyEvent.FACTORY, 32, Executors.newCachedThreadPool());
 * EventHandler&lt;MyEvent&gt; handler1 = new EventHandler&lt;MyEvent&gt;() { ... };
 * EventHandler&lt;MyEvent&gt; handler2 = new EventHandler&lt;MyEvent&gt;() { ... };
 * disruptor.handleEventsWith(handler1);
 * disruptor.after(handler1).handleEventsWith(handler2);
 *
 * RingBuffer ringBuffer = disruptor.start();</code>
 * </pre>
 *
 * 下面是一个使用两个必须按顺序处理事件的事件处理器来搭建 disruptor 的简单示例：
 *
 * @param <T> the type of event used.
 *
 * 泛型参数 T 是所使用事件的类型。
 */
public class Disruptor<T>
{
    private final RingBuffer<T> ringBuffer;
    private final ThreadFactory threadFactory;
    private final ConsumerRepository consumerRepository = new ConsumerRepository();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private ExceptionHandler<? super T> exceptionHandler = new ExceptionHandlerWrapper<>();

    /**
     * Create a new Disruptor. Will default to {@link com.lmax.disruptor.BlockingWaitStrategy} and
     * {@link ProducerType}.MULTI
     *
     * 创建一个新的 Disruptor。默认使用 BlockingWaitStrategy 和 ProducerType.MULTI。
     *
     * @param eventFactory   the factory to create events in the ring buffer.
     * @param ringBufferSize the size of the ring buffer.
     * @param threadFactory  a {@link ThreadFactory} to create threads to for processors.
     *
     * eventFactory 是用于在环形缓冲区中创建事件的工厂；ringBufferSize 是环形缓冲区的大小；threadFactory 是用于为处理器创建线程的 ThreadFactory。
     */
    public Disruptor(final EventFactory<T> eventFactory, final int ringBufferSize, final ThreadFactory threadFactory)
    {
        this(RingBuffer.createMultiProducer(eventFactory, ringBufferSize), threadFactory);
    }

    /**
     * Create a new Disruptor.
     *
     * 创建一个新的 Disruptor。
     *
     * @param eventFactory   the factory to create events in the ring buffer.
     * @param ringBufferSize the size of the ring buffer, must be power of 2.
     * @param threadFactory  a {@link ThreadFactory} to create threads for processors.
     * @param producerType   the claim strategy to use for the ring buffer.
     * @param waitStrategy   the wait strategy to use for the ring buffer.
     *
     * eventFactory 是用于在环形缓冲区中创建事件的工厂；ringBufferSize 是环形缓冲区的大小，必须是 2 的幂；threadFactory 是用于为处理器创建线程的 ThreadFactory；producerType 是环形缓冲区使用的申请（认领）策略；waitStrategy 是环形缓冲区使用的等待策略。
     */
    public Disruptor(
            final EventFactory<T> eventFactory,
            final int ringBufferSize,
            final ThreadFactory threadFactory,
            final ProducerType producerType,
            final WaitStrategy waitStrategy)
    {
        this(
            RingBuffer.create(producerType, eventFactory, ringBufferSize, waitStrategy),
            threadFactory);
    }

    /**
     * Private constructor helper
     *
     * 私有构造函数辅助方法。
     */
    private Disruptor(final RingBuffer<T> ringBuffer, final ThreadFactory threadFactory)
    {
        this.ringBuffer = ringBuffer;
        this.threadFactory = threadFactory;
    }

    /**
     * <p>Set up event handlers to handle events from the ring buffer. These handlers will process events
     * as soon as they become available, in parallel.</p>
     *
     * 设置事件处理器来处理来自环形缓冲区的事件。这些处理器会在事件可用后尽快并行处理它们。
     *
     * <p>This method can be used as the start of a chain. For example if the handler <code>A</code> must
     * process events before handler <code>B</code>:</p>
     * <pre><code>dw.handleEventsWith(A).then(B);</code></pre>
     *
     * 此方法可以作为一条链的起点。例如，如果处理器 A 必须先于处理器 B 处理事件：
     * dw.handleEventsWith(A).then(B);
     *
     * <p>This call is additive, but generally should only be called once when setting up the Disruptor instance</p>
     *
     * 该调用是可叠加的，但在搭建 Disruptor 实例时通常只应调用一次。
     *
     * @param handlers the event handlers that will process events.
     * @return a {@link EventHandlerGroup} that can be used to chain dependencies.
     *
     * handlers 是将处理事件的事件处理器；返回一个可用于串联依赖关系的 EventHandlerGroup。
     */
    @SuppressWarnings("varargs")
    @SafeVarargs
    public final EventHandlerGroup<T> handleEventsWith(final EventHandler<? super T>... handlers)
    {
        return createEventProcessors(new Sequence[0], handlers);
    }

    /**
     * <p>Set up event handlers to handle events from the ring buffer. These handlers will process events
     * as soon as they become available, in parallel.</p>
     *
     * 设置事件处理器来处理来自环形缓冲区的事件。这些处理器会在事件可用后尽快并行处理它们。
     *
     * <p>This method can be used as the start of a chain. For example if the handler <code>A</code> must
     * process events before handler <code>B</code>:</p>
     * <pre><code>dw.handleEventsWith(A).then(B);</code></pre>
     *
     * 此方法可以作为一条链的起点。例如，如果处理器 A 必须先于处理器 B 处理事件：
     * dw.handleEventsWith(A).then(B);
     *
     * <p>This call is additive, but generally should only be called once when setting up the Disruptor instance</p>
     *
     * 该调用是可叠加的，但在搭建 Disruptor 实例时通常只应调用一次。
     *
     * @param batchRewindStrategy a {@link BatchRewindStrategy} for customizing how to handle a {@link RewindableException}.
     * @param handlers            the rewindable event handlers that will process events.
     * @return a {@link EventHandlerGroup} that can be used to chain dependencies.
     *
     * batchRewindStrategy 是用于自定义如何处理 RewindableException 的批次回退策略；handlers 是将处理事件的可回退事件处理器；返回一个可用于串联依赖关系的 EventHandlerGroup。
     */
    @SuppressWarnings("varargs")
    @SafeVarargs
    public final EventHandlerGroup<T> handleEventsWith(final BatchRewindStrategy batchRewindStrategy,
                                                       final RewindableEventHandler<? super T>... handlers)
    {
        return createEventProcessors(new Sequence[0], batchRewindStrategy, handlers);
    }

    /**
     * <p>Set up custom event processors to handle events from the ring buffer. The Disruptor will
     * automatically start these processors when {@link #start()} is called.</p>
     *
     * 设置自定义事件处理器来处理来自环形缓冲区的事件。调用 start() 时 Disruptor 会自动启动这些处理器。
     *
     * <p>This method can be used as the start of a chain. For example if the handler <code>A</code> must
     * process events before handler <code>B</code>:</p>
     * <pre><code>dw.handleEventsWith(A).then(B);</code></pre>
     *
     * 此方法可以作为一条链的起点。例如，如果处理器 A 必须先于处理器 B 处理事件：
     * dw.handleEventsWith(A).then(B);
     *
     * <p>Since this is the start of the chain, the processor factories will always be passed an empty <code>Sequence</code>
     * array, so the factory isn't necessary in this case. This method is provided for consistency with
     * {@link EventHandlerGroup#handleEventsWith(EventProcessorFactory...)} and {@link EventHandlerGroup#then(EventProcessorFactory...)}
     * which do have barrier sequences to provide.</p>
     *
     * 由于这里是链的起点，处理器工厂始终会被传入一个空的序号数组，因此在这种情况下并不需要工厂。
     * 提供此方法是为了与 EventHandlerGroup#handleEventsWith(...) 和 EventHandlerGroup#then(...) 保持一致——那两个方法才有屏障序号可以提供。
     *
     * <p>This call is additive, but generally should only be called once when setting up the Disruptor instance</p>
     *
     * 该调用是可叠加的，但在搭建 Disruptor 实例时通常只应调用一次。
     *
     * @param eventProcessorFactories the event processor factories to use to create the event processors that will process events.
     * @return a {@link EventHandlerGroup} that can be used to chain dependencies.
     *
     * eventProcessorFactories 是用于创建事件处理器的工厂，这些事件处理器将处理事件；返回一个可用于串联依赖关系的 EventHandlerGroup。
     */
    @SafeVarargs
    public final EventHandlerGroup<T> handleEventsWith(final EventProcessorFactory<T>... eventProcessorFactories)
    {
        final Sequence[] barrierSequences = new Sequence[0];
        return createEventProcessors(barrierSequences, eventProcessorFactories);
    }

    /**
     * <p>Set up custom event processors to handle events from the ring buffer. The Disruptor will
     * automatically start this processors when {@link #start()} is called.</p>
     *
     * 设置自定义事件处理器来处理来自环形缓冲区的事件。调用 start() 时 Disruptor 会自动启动这些处理器。
     *
     * <p>This method can be used as the start of a chain. For example if the processor <code>A</code> must
     * process events before handler <code>B</code>:</p>
     * <pre><code>dw.handleEventsWith(A).then(B);</code></pre>
     *
     * 此方法可以作为一条链的起点。例如，如果处理器 A 必须先于处理器 B 处理事件：
     * dw.handleEventsWith(A).then(B);
     *
     * @param processors the event processors that will process events.
     * @return a {@link EventHandlerGroup} that can be used to chain dependencies.
     *
     * processors 是将处理事件的事件处理器；返回一个可用于串联依赖关系的 EventHandlerGroup。
     */
    public EventHandlerGroup<T> handleEventsWith(final EventProcessor... processors)
    {
        // 将处理器注册到消费者仓库
        for (final EventProcessor processor : processors)
        {
            consumerRepository.add(processor);
        }

        final Sequence[] sequences = Util.getSequencesFor(processors);

        // 把处理器序号加入环形缓冲区的门控序号，防止事件被覆盖
        ringBuffer.addGatingSequences(sequences);

        return new EventHandlerGroup<>(this, consumerRepository, sequences);
    }


    /**
     * <p>Specify an exception handler to be used for any future event handlers.</p>
     *
     * 指定一个供之后注册的事件处理器使用的异常处理器。
     *
     * <p>Note that only event handlers set up after calling this method will use the exception handler.</p>
     *
     * 注意：只有在此方法调用之后设置的事件处理器才会使用该异常处理器。
     *
     * @param exceptionHandler the exception handler to use for any future {@link EventProcessor}.
     * @deprecated This method only applies to future event handlers. Use setDefaultExceptionHandler instead which applies to existing and new event handlers.
     *
     * exceptionHandler 是供未来的 EventProcessor 使用的异常处理器。已废弃：此方法只对之后注册的事件处理器生效；请改用对已有和新增事件处理器都生效的 setDefaultExceptionHandler。
     */
    @Deprecated
    public void handleExceptionsWith(final ExceptionHandler<? super T> exceptionHandler)
    {
        this.exceptionHandler = exceptionHandler;
    }

    /**
     * <p>Specify an exception handler to be used for event handlers and worker pools created by this Disruptor.</p>
     *
     * 指定一个供此 Disruptor 创建的事件处理器和 worker pool 使用的异常处理器。
     *
     * <p>The exception handler will be used by existing and future event handlers and worker pools created by this Disruptor instance.</p>
     *
     * 该异常处理器将被此 Disruptor 实例已有的和将来创建的事件处理器及 worker pool 共同使用。
     *
     * @param exceptionHandler the exception handler to use.
     *
     * exceptionHandler 是要使用的异常处理器。
     */
    @SuppressWarnings("unchecked")
    public void setDefaultExceptionHandler(final ExceptionHandler<? super T> exceptionHandler)
    {
        checkNotStarted();
        if (!(this.exceptionHandler instanceof ExceptionHandlerWrapper))
        {
            throw new IllegalStateException("setDefaultExceptionHandler can not be used after handleExceptionsWith");
        }
        ((ExceptionHandlerWrapper<T>) this.exceptionHandler).switchTo(exceptionHandler);
    }

    /**
     * Override the default exception handler for a specific handler.
     * <pre>disruptorWizard.handleExceptionsIn(eventHandler).with(exceptionHandler);</pre>
     *
     * 为指定的处理器覆盖默认的异常处理器。
     * 示例：disruptorWizard.handleExceptionsIn(eventHandler).with(exceptionHandler);
     *
     * @param eventHandler the event handler to set a different exception handler for.
     * @return an ExceptionHandlerSetting dsl object - intended to be used by chaining the with method call.
     *
     * eventHandler 是要设置不同异常处理器的事件处理器；返回一个 ExceptionHandlerSetting DSL 对象，用于链式调用 with 方法。
     */
    public ExceptionHandlerSetting<T> handleExceptionsFor(final EventHandlerIdentity eventHandler)
    {
        return new ExceptionHandlerSetting<>(eventHandler, consumerRepository);
    }

    /**
     * <p>Create a group of event handlers to be used as a dependency.
     * For example if the handler <code>A</code> must process events before handler <code>B</code>:</p>
     *
     * <pre><code>dw.after(A).handleEventsWith(B);</code></pre>
     *
     * 创建一组用作依赖的事件处理器。例如，如果处理器 A 必须先于处理器 B 处理事件：
     * dw.after(A).handleEventsWith(B);
     *
     * @param handlers the event handlers, previously set up with {@link #handleEventsWith(EventHandler[])},
     *                 that will form the barrier for subsequent handlers or processors.
     * @return an {@link EventHandlerGroup} that can be used to setup a dependency barrier over the specified event handlers.
     *
     * handlers 是之前通过 handleEventsWith(EventHandler[]) 设置的事件处理器，它们将构成后续处理器的事件屏障；
     * 返回一个 EventHandlerGroup，可用于在指定事件处理器之上搭建依赖屏障。
     */
    public final EventHandlerGroup<T> after(final EventHandlerIdentity... handlers)
    {
        final Sequence[] sequences = new Sequence[handlers.length];
        for (int i = 0, handlersLength = handlers.length; i < handlersLength; i++)
        {
            sequences[i] = consumerRepository.getSequenceFor(handlers[i]);
        }

        return new EventHandlerGroup<>(this, consumerRepository, sequences);
    }

    /**
     * Create a group of event processors to be used as a dependency.
     *
     * 创建一组用作依赖的事件处理器。
     *
     * @param processors the event processors, previously set up with {@link #handleEventsWith(com.lmax.disruptor.EventProcessor...)},
     *                   that will form the barrier for subsequent handlers or processors.
     * @return an {@link EventHandlerGroup} that can be used to setup a {@link SequenceBarrier} over the specified event processors.
     * @see #after(EventHandlerIdentity[])
     *
     * processors 是之前通过 handleEventsWith(EventProcessor...) 设置的事件处理器，它们将构成后续处理器的事件屏障；
     * 返回一个 EventHandlerGroup，可用于在指定事件处理器之上搭建 SequenceBarrier。
     */
    public EventHandlerGroup<T> after(final EventProcessor... processors)
    {
        return new EventHandlerGroup<>(this, consumerRepository, Util.getSequencesFor(processors));
    }

    /**
     * Publish an event to the ring buffer.
     *
     * 向环形缓冲区发布一个事件。
     *
     * @param eventTranslator the translator that will load data into the event.
     *
     * eventTranslator 是将数据加载到事件中的 translator。
     */
    public void publishEvent(final EventTranslator<T> eventTranslator)
    {
        ringBuffer.publishEvent(eventTranslator);
    }

    /**
     * Publish an event to the ring buffer.
     *
     * 向环形缓冲区发布一个事件。
     *
     * @param <A>             Class of the user supplied argument.
     * @param eventTranslator the translator that will load data into the event.
     * @param arg             A single argument to load into the event
     *
     * 泛型参数 A 是用户传入参数的类；eventTranslator 是将数据加载到事件中的 translator；arg 是要加载到事件中的单个参数。
     */
    public <A> void publishEvent(final EventTranslatorOneArg<T, A> eventTranslator, final A arg)
    {
        ringBuffer.publishEvent(eventTranslator, arg);
    }

    /**
     * Publish a batch of events to the ring buffer.
     *
     * 向环形缓冲区发布一批事件。
     *
     * @param <A>             Class of the user supplied argument.
     * @param eventTranslator the translator that will load data into the event.
     * @param arg             An array single arguments to load into the events. One Per event.
     *
     * 泛型参数 A 是用户传入参数的类；eventTranslator 是将数据加载到事件中的 translator；arg 是要加载到各事件中的参数数组，每个事件对应一个参数。
     */
    public <A> void publishEvents(final EventTranslatorOneArg<T, A> eventTranslator, final A[] arg)
    {
        ringBuffer.publishEvents(eventTranslator, arg);
    }

    /**
     * Publish an event to the ring buffer.
     *
     * 向环形缓冲区发布一个事件。
     *
     * @param <A>             Class of the user supplied argument.
     * @param <B>             Class of the user supplied argument.
     * @param eventTranslator the translator that will load data into the event.
     * @param arg0            The first argument to load into the event
     * @param arg1            The second argument to load into the event
     *
     * 泛型参数 A、B 是用户传入参数的类；eventTranslator 是将数据加载到事件中的 translator；arg0 是要加载到事件中的第一个参数；arg1 是第二个参数。
     */
    public <A, B> void publishEvent(final EventTranslatorTwoArg<T, A, B> eventTranslator, final A arg0, final B arg1)
    {
        ringBuffer.publishEvent(eventTranslator, arg0, arg1);
    }

    /**
     * Publish an event to the ring buffer.
     *
     * 向环形缓冲区发布一个事件。
     *
     * @param eventTranslator the translator that will load data into the event.
     * @param <A>             Class of the user supplied argument.
     * @param <B>             Class of the user supplied argument.
     * @param <C>             Class of the user supplied argument.
     * @param arg0            The first argument to load into the event
     * @param arg1            The second argument to load into the event
     * @param arg2            The third argument to load into the event
     *
     * eventTranslator 是将数据加载到事件中的 translator；泛型参数 A、B、C 是用户传入参数的类；
     * arg0、arg1、arg2 分别是要加载到事件中的第一、第二、第三个参数。
     */
    public <A, B, C> void publishEvent(final EventTranslatorThreeArg<T, A, B, C> eventTranslator, final A arg0, final B arg1, final C arg2)
    {
        ringBuffer.publishEvent(eventTranslator, arg0, arg1, arg2);
    }

    /**
     * <p>Starts the event processors and returns the fully configured ring buffer.</p>
     *
     * 启动所有事件处理器，并返回配置完成的环形缓冲区。
     *
     * <p>The ring buffer is set up to prevent overwriting any entry that is yet to
     * be processed by the slowest event processor.</p>
     *
     * 环形缓冲区被配置为防止覆盖任何尚未被最慢的事件处理器处理的条目。
     *
     * <p>This method must only be called once after all event processors have been added.</p>
     *
     * 此方法必须在所有事件处理器都添加完成后调用，且只能调用一次。
     *
     * @return the configured ring buffer.
     *
     * 返回配置完成的环形缓冲区。
     */
    public RingBuffer<T> start()
    {
        checkOnlyStartedOnce();
        consumerRepository.startAll(threadFactory);

        return ringBuffer;
    }

    /**
     * Calls {@link com.lmax.disruptor.EventProcessor#halt()} on all of the event processors created via this disruptor.
     *
     * 对通过此 disruptor 创建的所有事件处理器调用 EventProcessor#halt()。
     */
    public void halt()
    {
        consumerRepository.haltAll();
    }

    /**
     * <p>Waits until all events currently in the disruptor have been processed by all event processors
     * and then halts the processors.  It is critical that publishing to the ring buffer has stopped
     * before calling this method, otherwise it may never return.</p>
     *
     * 等待当前 disruptor 中的所有事件都被所有事件处理器处理完毕，然后停止这些处理器。
     * 调用此方法前必须已经停止向环形缓冲区发布事件，否则该方法可能永远不会返回。
     *
     * <p>This method will not shutdown the executor, nor will it await the final termination of the
     * processor threads.</p>
     *
     * 此方法不会关闭执行器，也不会等待处理器线程最终终止。
     */
    public void shutdown()
    {
        try
        {
            shutdown(-1, TimeUnit.MILLISECONDS);
        }
        catch (final TimeoutException e)
        {
            exceptionHandler.handleOnShutdownException(e);
        }
    }

    /**
     * <p>Waits until all events currently in the disruptor have been processed by all event processors
     * and then halts the processors.</p>
     *
     * 等待当前 disruptor 中的所有事件都被所有事件处理器处理完毕，然后停止这些处理器。
     *
     * <p>This method will not shutdown the executor, nor will it await the final termination of the
     * processor threads.</p>
     *
     * 此方法不会关闭执行器，也不会等待处理器线程最终终止。
     *
     * @param timeout  the amount of time to wait for all events to be processed. <code>-1</code> will give an infinite timeout
     * @param timeUnit the unit the timeOut is specified in
     * @throws TimeoutException if a timeout occurs before shutdown completes.
     *
     * timeout 是等待所有事件被处理完毕的时长，-1 表示无限等待；timeUnit 是 timeout 的时间单位；如果停机完成前发生超时则抛出 TimeoutException。
     */
    public void shutdown(final long timeout, final TimeUnit timeUnit) throws TimeoutException
    {
        final long timeOutAt = System.nanoTime() + timeUnit.toNanos(timeout);
        while (hasBacklog())
        {
            if (timeout >= 0 && System.nanoTime() > timeOutAt)
            {
                throw TimeoutException.INSTANCE;
            }
            // Busy spin
            //
            // 忙等待自旋
        }
        halt();
    }

    /**
     * The {@link RingBuffer} used by this Disruptor.  This is useful for creating custom
     * event processors if the behaviour of {@link BatchEventProcessor} is not suitable.
     *
     * 此 Disruptor 使用的环形缓冲区。当 BatchEventProcessor 的行为不合适时，可用它来创建自定义事件处理器。
     *
     * @return the ring buffer used by this Disruptor.
     *
     * 返回此 Disruptor 使用的环形缓冲区。
     */
    public RingBuffer<T> getRingBuffer()
    {
        return ringBuffer;
    }

    /**
     * Get the value of the cursor indicating the published sequence.
     *
     * 获取表示已发布序号的游标值。
     *
     * @return value of the cursor for events that have been published.
     *
     * 返回已发布事件对应的游标值。
     */
    public long getCursor()
    {
        return ringBuffer.getCursor();
    }

    /**
     * The capacity of the data structure to hold entries.
     *
     * 该数据结构可容纳条目的容量。
     *
     * @return the size of the RingBuffer.
     * @see com.lmax.disruptor.Sequencer#getBufferSize()
     *
     * 返回 RingBuffer 的大小。
     */
    public long getBufferSize()
    {
        return ringBuffer.getBufferSize();
    }

    /**
     * Get the event for a given sequence in the RingBuffer.
     *
     * 获取 RingBuffer 中指定序号对应的事件。
     *
     * @param sequence for the event.
     * @return event for the sequence.
     * @see RingBuffer#get(long)
     *
     * sequence 是事件的序号；返回该序号对应的事件。
     */
    public T get(final long sequence)
    {
        return ringBuffer.get(sequence);
    }

    /**
     * Get the {@link SequenceBarrier} used by a specific handler. Note that the {@link SequenceBarrier}
     * may be shared by multiple event handlers.
     *
     * 获取指定处理器使用的 SequenceBarrier。注意，SequenceBarrier 可能被多个事件处理器共享。
     *
     * @param handler the handler to get the barrier for.
     * @return the SequenceBarrier used by <i>handler</i>.
     *
     * handler 是要获取屏障的处理器；返回该 handler 使用的 SequenceBarrier。
     */
    public SequenceBarrier getBarrierFor(final EventHandlerIdentity handler)
    {
        return consumerRepository.getBarrierFor(handler);
    }

    /**
     * Gets the sequence value for the specified event handlers.
     *
     * 获取指定事件处理器的序号值。
     *
     * @param handler eventHandler to get the sequence for.
     * @return eventHandler's sequence
     *
     * handler 是要获取序号的事件处理器；返回该事件处理器的序号。
     */
    public long getSequenceValueFor(final EventHandlerIdentity handler)
    {
        return consumerRepository.getSequenceFor(handler).get();
    }

    /**
     * Confirms if all messages have been consumed by all event processors
     *
     * 确认是否所有消息都已被所有事件处理器消费。
     */
    private boolean hasBacklog()
    {
        final long cursor = ringBuffer.getCursor();

        return consumerRepository.hasBacklog(cursor, false);
    }

    /**
     * Checks if disruptor has been started
     *
     * 检查 disruptor 是否已启动。
     *
     * @return true when start has been called on this instance; otherwise false
     *
     * 当此实例已调用过 start 时返回 true，否则返回 false。
     */
    public boolean hasStarted()
    {
        return started.get();
    }

    /**
     * Create event processors for the given event handlers with the given barrier sequences.
     *
     * 使用给定的屏障序号为给定的事件处理器创建批事件处理器。
     */
    EventHandlerGroup<T> createEventProcessors(
            final Sequence[] barrierSequences,
            final EventHandler<? super T>[] eventHandlers)
    {
        checkNotStarted();

        // 基于屏障序号创建序号屏障
        final Sequence[] processorSequences = new Sequence[eventHandlers.length];
        final SequenceBarrier barrier = ringBuffer.newBarrier(barrierSequences);

        // 逐个为处理器构建批事件处理器，注册到消费者仓库并记录其序号
        for (int i = 0, eventHandlersLength = eventHandlers.length; i < eventHandlersLength; i++)
        {
            final EventHandler<? super T> eventHandler = eventHandlers[i];

            final BatchEventProcessor<T> batchEventProcessor =
                    new BatchEventProcessorBuilder().build(ringBuffer, barrier, eventHandler);

            if (exceptionHandler != null)
            {
                batchEventProcessor.setExceptionHandler(exceptionHandler);
            }

            consumerRepository.add(batchEventProcessor, eventHandler, barrier);
            processorSequences[i] = batchEventProcessor.getSequence();
        }

        // 用新处理器的序号替换链中旧的门控序号
        updateGatingSequencesForNextInChain(barrierSequences, processorSequences);

        return new EventHandlerGroup<>(this, consumerRepository, processorSequences);
    }

    /**
     * Create rewindable event processors for the given event handlers with the given batch rewind strategy.
     *
     * 使用给定的批次回退策略为给定的事件处理器创建可回退的批事件处理器。
     */
    EventHandlerGroup<T> createEventProcessors(
            final Sequence[] barrierSequences,
            final BatchRewindStrategy batchRewindStrategy,
            final RewindableEventHandler<? super T>[] eventHandlers)
    {
        checkNotStarted();

        // 基于屏障序号创建序号屏障
        final Sequence[] processorSequences = new Sequence[eventHandlers.length];
        final SequenceBarrier barrier = ringBuffer.newBarrier(barrierSequences);

        // 逐个为处理器构建可回退的批事件处理器，注册到消费者仓库并记录其序号
        for (int i = 0, eventHandlersLength = eventHandlers.length; i < eventHandlersLength; i++)
        {
            final RewindableEventHandler<? super T> eventHandler = eventHandlers[i];

            final BatchEventProcessor<T> batchEventProcessor =
                    new BatchEventProcessorBuilder().build(ringBuffer, barrier, eventHandler, batchRewindStrategy);

            if (exceptionHandler != null)
            {
                batchEventProcessor.setExceptionHandler(exceptionHandler);
            }

            consumerRepository.add(batchEventProcessor, eventHandler, barrier);
            processorSequences[i] = batchEventProcessor.getSequence();
        }

        // 用新处理器的序号替换链中旧的门控序号
        updateGatingSequencesForNextInChain(barrierSequences, processorSequences);

        return new EventHandlerGroup<>(this, consumerRepository, processorSequences);
    }

    /**
     * Replace the barrier sequences with the new processor sequences as the gating sequences of the ring buffer.
     *
     * 用新处理器的序号替换环形缓冲区门控序号中对应的屏障序号。
     */
    private void updateGatingSequencesForNextInChain(final Sequence[] barrierSequences, final Sequence[] processorSequences)
    {
        if (processorSequences.length > 0)
        {
            // 将新处理器序号设为门控序号，移除被替代的屏障序号并取消其链尾标记
            ringBuffer.addGatingSequences(processorSequences);
            for (final Sequence barrierSequence : barrierSequences)
            {
                ringBuffer.removeGatingSequence(barrierSequence);
            }
            consumerRepository.unMarkEventProcessorsAsEndOfChain(barrierSequences);
        }
    }

    /**
     * Create event processors from the given processor factories with the given barrier sequences.
     *
     * 使用给定的屏障序号，通过给定的处理器工厂创建事件处理器。
     */
    EventHandlerGroup<T> createEventProcessors(
            final Sequence[] barrierSequences, final EventProcessorFactory<T>[] processorFactories)
    {
        // 逐个调用工厂创建事件处理器
        final EventProcessor[] eventProcessors = new EventProcessor[processorFactories.length];
        for (int i = 0; i < processorFactories.length; i++)
        {
            eventProcessors[i] = processorFactories[i].createEventProcessor(ringBuffer, barrierSequences);
        }

        return handleEventsWith(eventProcessors);
    }

    /**
     * Ensure the disruptor has not been started yet.
     *
     * 确保 disruptor 尚未启动。
     */
    private void checkNotStarted()
    {
        if (started.get())
        {
            throw new IllegalStateException("All event handlers must be added before calling starts.");
        }
    }

    /**
     * Ensure that start has only been called once.
     *
     * 确保 start 只被调用过一次。
     */
    private void checkOnlyStartedOnce()
    {
        if (!started.compareAndSet(false, true))
        {
            throw new IllegalStateException("Disruptor.start() must only be called once.");
        }
    }

    /**
     * Returns a string representation of this disruptor.
     *
     * 返回此 disruptor 的字符串表示。
     */
    @Override
    public String toString()
    {
        return "Disruptor{" +
                "ringBuffer=" + ringBuffer +
                ", started=" + started +
                ", threadFactory=" + threadFactory +
                '}';
    }
}
