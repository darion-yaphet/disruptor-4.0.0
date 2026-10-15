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
package com.lmax.disruptor;


import com.lmax.disruptor.dsl.ProducerType;

/**
 * Padding used to keep the ring buffer fields on their own cache line.
 *
 * 填充类，用于让环形缓冲区字段独占缓存行，避免伪共享（false sharing）。
 */
abstract class RingBufferPad
{
    protected byte
        p10, p11, p12, p13, p14, p15, p16, p17,
        p20, p21, p22, p23, p24, p25, p26, p27,
        p30, p31, p32, p33, p34, p35, p36, p37,
        p40, p41, p42, p43, p44, p45, p46, p47,
        p50, p51, p52, p53, p54, p55, p56, p57,
        p60, p61, p62, p63, p64, p65, p66, p67,
        p70, p71, p72, p73, p74, p75, p76, p77;
}

/**
 * Holds the actual entries array and the sequencer, sandwiched between padding.
 *
 * 保存实际的条目数组和序号器，前后由填充字节包围以隔离缓存行。
 */
abstract class RingBufferFields<E> extends RingBufferPad
{
    private static final int BUFFER_PAD = 32;

    private final long indexMask;
    private final E[] entries;
    protected final int bufferSize;
    protected final Sequencer sequencer;

    /**
     * Creates the ring buffer storage and pre-allocates all event entries.
     *
     * 创建环形缓冲区存储，并预先分配好所有事件条目。
     */
    @SuppressWarnings("unchecked")
    RingBufferFields(
        final EventFactory<E> eventFactory,
        final Sequencer sequencer)
    {
        this.sequencer = sequencer;
        this.bufferSize = sequencer.getBufferSize();

        if (bufferSize < 1)
        {
            throw new IllegalArgumentException("bufferSize must not be less than 1");
        }
        if (Integer.bitCount(bufferSize) != 1)
        {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }

        this.indexMask = bufferSize - 1;
        this.entries = (E[]) new Object[bufferSize + 2 * BUFFER_PAD];
        fill(eventFactory);
    }

    /**
     * Pre-fills every slot of the ring buffer with a newly created event instance.
     *
     * 用新建的事件实例预先填满环形缓冲区的每个槽位。
     */
    private void fill(final EventFactory<E> eventFactory)
    {
        for (int i = 0; i < bufferSize; i++)
        {
            entries[BUFFER_PAD + i] = eventFactory.newInstance();
        }
    }

    /**
     * Locates the preallocated entry for the given sequence via the index mask.
     *
     * 通过索引掩码定位给定序号对应的预分配条目。
     */
    protected final E elementAt(final long sequence)
    {
        return entries[BUFFER_PAD + (int) (sequence & indexMask)];
    }
}

/**
 * Ring based store of reusable entries containing the data representing
 * an event being exchanged between event producer and {@link EventProcessor}s.
 *
 * 基于环形结构的可复用条目存储，条目承载着在事件生产者与 EventProcessor 之间交换的事件数据。
 *
 * @param <E> implementation storing the data for sharing during exchange or parallel coordination of an event.
 *
 * 参数 <E>：在事件交换或并行协调过程中存储共享数据的实现类型。
 */
public final class RingBuffer<E> extends RingBufferFields<E> implements Cursored, EventSequencer<E>, EventSink<E>
{
    /**
     * The initial cursor value
     *
     * 游标的初始值
     */
    public static final long INITIAL_CURSOR_VALUE = Sequence.INITIAL_VALUE;
    protected byte
        p10, p11, p12, p13, p14, p15, p16, p17,
        p20, p21, p22, p23, p24, p25, p26, p27,
        p30, p31, p32, p33, p34, p35, p36, p37,
        p40, p41, p42, p43, p44, p45, p46, p47,
        p50, p51, p52, p53, p54, p55, p56, p57,
        p60, p61, p62, p63, p64, p65, p66, p67,
        p70, p71, p72, p73, p74, p75, p76, p77;

    /**
     * Construct a RingBuffer with the full option set.
     *
     * 以完整选项集构造一个 RingBuffer。
     *
     * @param eventFactory to newInstance entries for filling the RingBuffer
     * @param sequencer    sequencer to handle the ordering of events moving through the RingBuffer.
     * @throws IllegalArgumentException if bufferSize is less than 1 or not a power of 2
     *
     * eventFactory：用于创建填充 RingBuffer 的条目实例；sequencer：处理事件流经 RingBuffer 时的排序。
     * 若 bufferSize 小于 1 或不是 2 的幂，则抛出 IllegalArgumentException。
     */
    RingBuffer(
        final EventFactory<E> eventFactory,
        final Sequencer sequencer)
    {
        super(eventFactory, sequencer);
    }

    /**
     * Create a new multiple producer RingBuffer with the specified wait strategy.
     *
     * 使用指定的等待策略创建一个新的多生产者 RingBuffer。
     *
     * @param <E> Class of the event stored in the ring buffer.
     * @param factory      used to create the events within the ring buffer.
     * @param bufferSize   number of elements to create within the ring buffer.
     * @param waitStrategy used to determine how to wait for new elements to become available.
     * @return a constructed ring buffer.
     * @throws IllegalArgumentException if bufferSize is less than 1 or not a power of 2
     * @see MultiProducerSequencer
     *
     * 参数 <E>：环形缓冲区中存储的事件类型；factory：用于创建环形缓冲区中的事件；
     * bufferSize：环形缓冲区中创建的元素个数；waitStrategy：用于决定如何等待新元素可用。
     * 返回构建好的环形缓冲区。若 bufferSize 小于 1 或不是 2 的幂，则抛出 IllegalArgumentException。
     */
    public static <E> RingBuffer<E> createMultiProducer(
        final EventFactory<E> factory,
        final int bufferSize,
        final WaitStrategy waitStrategy)
    {
        MultiProducerSequencer sequencer = new MultiProducerSequencer(bufferSize, waitStrategy);

        return new RingBuffer<>(factory, sequencer);
    }

    /**
     * Create a new multiple producer RingBuffer using the default wait strategy  {@link BlockingWaitStrategy}.
     *
     * 使用默认等待策略 BlockingWaitStrategy 创建一个新的多生产者 RingBuffer。
     *
     * @param <E> Class of the event stored in the ring buffer.
     * @param factory    used to create the events within the ring buffer.
     * @param bufferSize number of elements to create within the ring buffer.
     * @return a constructed ring buffer.
     * @throws IllegalArgumentException if <code>bufferSize</code> is less than 1 or not a power of 2
     * @see MultiProducerSequencer
     *
     * 参数 <E>：环形缓冲区中存储的事件类型；factory：用于创建环形缓冲区中的事件；
     * bufferSize：环形缓冲区中创建的元素个数。返回构建好的环形缓冲区。
     * 若 bufferSize 小于 1 或不是 2 的幂，则抛出 IllegalArgumentException。
     */
    public static <E> RingBuffer<E> createMultiProducer(final EventFactory<E> factory, final int bufferSize)
    {
        return createMultiProducer(factory, bufferSize, new BlockingWaitStrategy());
    }

    /**
     * Create a new single producer RingBuffer with the specified wait strategy.
     *
     * 使用指定的等待策略创建一个新的单生产者 RingBuffer。
     *
     * @param <E> Class of the event stored in the ring buffer.
     * @param factory      used to create the events within the ring buffer.
     * @param bufferSize   number of elements to create within the ring buffer.
     * @param waitStrategy used to determine how to wait for new elements to become available.
     * @return a constructed ring buffer.
     * @throws IllegalArgumentException if bufferSize is less than 1 or not a power of 2
     * @see SingleProducerSequencer
     *
     * 参数 <E>：环形缓冲区中存储的事件类型；factory：用于创建环形缓冲区中的事件；
     * bufferSize：环形缓冲区中创建的元素个数；waitStrategy：用于决定如何等待新元素可用。
     * 返回构建好的环形缓冲区。若 bufferSize 小于 1 或不是 2 的幂，则抛出 IllegalArgumentException。
     */
    public static <E> RingBuffer<E> createSingleProducer(
        final EventFactory<E> factory,
        final int bufferSize,
        final WaitStrategy waitStrategy)
    {
        SingleProducerSequencer sequencer = new SingleProducerSequencer(bufferSize, waitStrategy);

        return new RingBuffer<>(factory, sequencer);
    }

    /**
     * Create a new single producer RingBuffer using the default wait strategy  {@link BlockingWaitStrategy}.
     *
     * 使用默认等待策略 BlockingWaitStrategy 创建一个新的单生产者 RingBuffer。
     *
     * @param <E> Class of the event stored in the ring buffer.
     * @param factory    used to create the events within the ring buffer.
     * @param bufferSize number of elements to create within the ring buffer.
     * @return a constructed ring buffer.
     * @throws IllegalArgumentException if <code>bufferSize</code> is less than 1 or not a power of 2
     * @see MultiProducerSequencer
     *
     * 参数 <E>：环形缓冲区中存储的事件类型；factory：用于创建环形缓冲区中的事件；
     * bufferSize：环形缓冲区中创建的元素个数。返回构建好的环形缓冲区。
     * 若 bufferSize 小于 1 或不是 2 的幂，则抛出 IllegalArgumentException。
     */
    public static <E> RingBuffer<E> createSingleProducer(final EventFactory<E> factory, final int bufferSize)
    {
        return createSingleProducer(factory, bufferSize, new BlockingWaitStrategy());
    }

    /**
     * Create a new Ring Buffer with the specified producer type (SINGLE or MULTI)
     *
     * 按指定的生产者类型（SINGLE 或 MULTI）创建一个新的 RingBuffer。
     *
     * @param <E> Class of the event stored in the ring buffer.
     * @param producerType producer type to use {@link ProducerType}.
     * @param factory      used to create events within the ring buffer.
     * @param bufferSize   number of elements to create within the ring buffer.
     * @param waitStrategy used to determine how to wait for new elements to become available.
     * @return a constructed ring buffer.
     * @throws IllegalArgumentException if bufferSize is less than 1 or not a power of 2
     *
     * 参数 <E>：环形缓冲区中存储的事件类型；producerType：使用的生产者类型 ProducerType；
     * factory：用于创建环形缓冲区中的事件；bufferSize：环形缓冲区中创建的元素个数；
     * waitStrategy：用于决定如何等待新元素可用。返回构建好的环形缓冲区。
     * 若 bufferSize 小于 1 或不是 2 的幂，则抛出 IllegalArgumentException。
     */
    public static <E> RingBuffer<E> create(
        final ProducerType producerType,
        final EventFactory<E> factory,
        final int bufferSize,
        final WaitStrategy waitStrategy)
    {
        switch (producerType)
        {
            case SINGLE:
                return createSingleProducer(factory, bufferSize, waitStrategy);
            case MULTI:
                return createMultiProducer(factory, bufferSize, waitStrategy);
            default:
                throw new IllegalStateException(producerType.toString());
        }
    }

    /**
     * <p>Get the event for a given sequence in the RingBuffer.</p>
     *
     * 获取 RingBuffer 中给定序号对应的事件。
     *
     * <p>This call has 2 uses.  Firstly use this call when publishing to a ring buffer.
     * After calling {@link RingBuffer#next()} use this call to get hold of the
     * preallocated event to fill with data before calling {@link RingBuffer#publish(long)}.</p>
     *
     * 该调用有两种用途。其一是在向环形缓冲区发布数据时使用：调用 RingBuffer#next() 之后，
     * 通过本调用取得预分配的事件并填入数据，然后再调用 RingBuffer#publish(long)。
     *
     * <p>Secondly use this call when consuming data from the ring buffer.  After calling
     * {@link SequenceBarrier#waitFor(long)} call this method with any value greater than
     * that your current consumer sequence and less than or equal to the value returned from
     * the {@link SequenceBarrier#waitFor(long)} method.</p>
     *
     * 其二是在从环形缓冲区消费数据时使用：调用 SequenceBarrier#waitFor(long) 之后，
     * 以任何大于当前消费者序号、且小于等于 waitFor(long) 返回值的序号调用本方法。
     *
     * @param sequence for the event
     * @return the event for the given sequence
     *
     * sequence：事件对应的序号；返回给定序号对应的事件。
     */
    @Override
    public E get(final long sequence)
    {
        return elementAt(sequence);
    }

    /**
     * Increment and return the next sequence for the ring buffer.  Calls of this
     * method should ensure that they always publish the sequence afterward.  E.g.
     * <pre>
     * long sequence = ringBuffer.next();
     * try {
     *     Event e = ringBuffer.get(sequence);
     *     // Do some work with the event.
     * } finally {
     *     ringBuffer.publish(sequence);
     * }
     * </pre>
     *
     * 递增并返回环形缓冲区的下一个序号。调用本方法后必须保证随后总是发布该序号，
     * 推荐按上面的示例写法使用 try/finally。
     *
     * @return The next sequence to publish to.
     * @see RingBuffer#publish(long)
     * @see RingBuffer#get(long)
     *
     * 返回下一个要发布到的序号。
     */
    @Override
    public long next()
    {
        return sequencer.next();
    }

    /**
     * The same functionality as {@link RingBuffer#next()}, but allows the caller to claim
     * the next n sequences.
     *
     * 功能与 RingBuffer#next() 相同，但允许调用方一次性申请接下来的 n 个序号。
     *
     * @param n number of slots to claim
     * @return sequence number of the highest slot claimed
     * @see Sequencer#next(int)
     *
     * n：要申请的槽位数；返回本次申请到的最高槽位序号。
     */
    @Override
    public long next(final int n)
    {
        return sequencer.next(n);
    }

    /**
     * <p>Increment and return the next sequence for the ring buffer.  Calls of this
     * method should ensure that they always publish the sequence afterward.  E.g.</p>
     * <pre>
     * long sequence = ringBuffer.next();
     * try {
     *     Event e = ringBuffer.get(sequence);
     *     // Do some work with the event.
     * } finally {
     *     ringBuffer.publish(sequence);
     * }
     * </pre>
     * <p>This method will not block if there is not space available in the ring
     * buffer, instead it will throw an {@link InsufficientCapacityException}.</p>
     *
     * 递增并返回环形缓冲区的下一个序号，调用后必须保证随后发布该序号（用法同上例）。
     * 与 next() 不同的是，当环形缓冲区空间不足时本方法不会阻塞，
     * 而是抛出 InsufficientCapacityException。
     *
     * @return The next sequence to publish to.
     * @throws InsufficientCapacityException if the necessary space in the ring buffer is not available
     * @see RingBuffer#publish(long)
     * @see RingBuffer#get(long)
     *
     * 返回下一个要发布到的序号；若环形缓冲区空间不足则抛出 InsufficientCapacityException。
     */
    @Override
    public long tryNext() throws InsufficientCapacityException
    {
        return sequencer.tryNext();
    }

    /**
     * The same functionality as {@link RingBuffer#tryNext()}, but allows the caller to attempt
     * to claim the next n sequences.
     *
     * 功能与 RingBuffer#tryNext() 相同，但允许调用方尝试一次性申请接下来的 n 个序号。
     *
     * @param n number of slots to claim
     * @return sequence number of the highest slot claimed
     * @throws InsufficientCapacityException if the necessary space in the ring buffer is not available
     *
     * n：要申请的槽位数；返回本次申请到的最高槽位序号；
     * 若环形缓冲区空间不足则抛出 InsufficientCapacityException。
     */
    @Override
    public long tryNext(final int n) throws InsufficientCapacityException
    {
        return sequencer.tryNext(n);
    }

    /**
     * Sets the cursor to a specific sequence and returns the preallocated entry that is stored there.  This
     * can cause a data race and should only be done in controlled circumstances, e.g. during initialisation.
     *
     * 将游标设置为指定序号，并返回该位置上存储的预分配条目。这可能引发数据竞争，
     * 只应在受控场景（例如初始化期间）下使用。
     *
     * @param sequence The sequence to claim.
     * @return The preallocated event.
     *
     * sequence：要认领的序号；返回预分配的事件。
     */
    public E claimAndGetPreallocated(final long sequence)
    {
        sequencer.claim(sequence);
        return get(sequence);
    }

    /**
     * Determines if the event for a given sequence is currently available.
     *
     * <p>Note that this does not guarantee that event will still be available
     * on the next interaction with the RingBuffer. For example, it is not
     * necessarily safe to write code like this:
     *
     * <pre>{@code
     * if (ringBuffer.isAvailable(sequence))
     * {
     *     final E e = ringBuffer.get(sequence);
     *     // ...do something with e
     * }
     * }</pre>
     *
     * <p>because there is a race between the reading thread and the writing thread.
     *
     * 判断给定序号对应的事件当前是否可用。注意这并不保证下次与 RingBuffer 交互时该事件
     * 仍然可用，例如上面那样的写法并不必然安全，因为读线程与写线程之间存在竞争。
     *
     * <p>This method will also return false when querying for sequences that are
     * behind the ring buffer's wrap point.
     *
     * 当查询的序号已经落后于环形缓冲区的回绕点（wrap point）时，本方法也会返回 false。
     *
     * @param sequence The sequence to identify the entry.
     * @return If the event published with the given sequence number is currently available.
     *
     * sequence：用于定位条目的序号；返回以该序号发布的事件当前是否可用。
     */
    public boolean isAvailable(final long sequence)
    {
        return sequencer.isAvailable(sequence);
    }

    /**
     * Add the specified gating sequences to this instance of the Disruptor.  They will
     * safely and atomically added to the list of gating sequences.
     *
     * 将指定的门控序号添加到本 Disruptor 实例，它们会被安全且原子地加入门控序号列表。
     *
     * @param gatingSequences The sequences to add.
     *
     * gatingSequences：要添加的序号。
     */
    public void addGatingSequences(final Sequence... gatingSequences)
    {
        sequencer.addGatingSequences(gatingSequences);
    }

    /**
     * Get the minimum sequence value from all of the gating sequences
     * added to this ringBuffer.
     *
     * 获取添加到本 ringBuffer 的所有门控序号中的最小值。
     *
     * @return The minimum gating sequence or the cursor sequence if
     * no sequences have been added.
     *
     * 返回最小的门控序号；若尚未添加任何序号，则返回游标序号。
     */
    public long getMinimumGatingSequence()
    {
        return sequencer.getMinimumSequence();
    }

    /**
     * Remove the specified sequence from this ringBuffer.
     *
     * 从本 ringBuffer 中移除指定的序号。
     *
     * @param sequence to be removed.
     * @return <code>true</code> if this sequence was found, <code>false</code> otherwise.
     *
     * sequence：要移除的序号；找到并移除则返回 true，否则返回 false。
     */
    public boolean removeGatingSequence(final Sequence sequence)
    {
        return sequencer.removeGatingSequence(sequence);
    }

    /**
     * Create a new SequenceBarrier to be used by an EventProcessor to track which messages
     * are available to be read from the ring buffer given a list of sequences to track.
     *
     * 创建一个新的 SequenceBarrier，供 EventProcessor 在给定一组待跟踪序号的情况下，
     * 跟踪环形缓冲区中哪些消息已经可读。
     *
     * @param sequencesToTrack the additional sequences to track
     * @return A sequence barrier that will track the specified sequences.
     * @see SequenceBarrier
     *
     * sequencesToTrack：额外需要跟踪的序号；返回跟踪指定序号的序号屏障。
     */
    public SequenceBarrier newBarrier(final Sequence... sequencesToTrack)
    {
        return sequencer.newBarrier(sequencesToTrack);
    }

    /**
     * Creates an event poller for this ring buffer gated on the supplied sequences.
     *
     * 为本环形缓冲区创建一个以所给序号为门控条件的事件轮询器（poller）。
     *
     * @param gatingSequences to be gated on.
     * @return A poller that will gate on this ring buffer and the supplied sequences.
     *
     * gatingSequences：作为门控条件的序号；返回基于本环形缓冲区和所给序号进行门控的轮询器。
     */
    public EventPoller<E> newPoller(final Sequence... gatingSequences)
    {
        return sequencer.newPoller(this, gatingSequences);
    }

    /**
     * Get the current cursor value for the ring buffer.  The actual value received
     * will depend on the type of {@link Sequencer} that is being used.
     *
     * 获取环形缓冲区的当前游标值。实际取到的值取决于所使用的 Sequencer 类型。
     *
     * @see MultiProducerSequencer
     * @see SingleProducerSequencer
     */
    @Override
    public long getCursor()
    {
        return sequencer.getCursor();
    }

    /**
     * The size of the buffer.
     *
     * 缓冲区的大小。
     *
     * @return size of buffer
     *
     * 返回缓冲区大小。
     */
    @Override
    public int getBufferSize()
    {
        return bufferSize;
    }

    /**
     * Given specified <code>requiredCapacity</code> determines if that amount of space
     * is available.  Note, you can not assume that if this method returns <code>true</code>
     * that a call to {@link RingBuffer#next()} will not block.  Especially true if this
     * ring buffer is set up to handle multiple producers.
     *
     * 判断环形缓冲区当前是否有指定的 requiredCapacity 大小的可用空间。注意，即使本方法
     * 返回 true，也不能假定随后调用 RingBuffer#next() 一定不会阻塞——当环形缓冲区
     * 配置为多生产者时尤其如此。
     *
     * @param requiredCapacity The capacity to check for.
     * @return <code>true</code> If the specified <code>requiredCapacity</code> is available
     * <code>false</code> if not.
     *
     * requiredCapacity：要检查的容量；若指定的容量可用则返回 true，否则返回 false。
     */
    @Override
    public boolean hasAvailableCapacity(final int requiredCapacity)
    {
        return sequencer.hasAvailableCapacity(requiredCapacity);
    }


    /**
     * @see com.lmax.disruptor.EventSink#publishEvent(com.lmax.disruptor.EventTranslator)
     *
     * 发布单个事件：申请下一个序号后，用 translator 填充事件并发布。
     */
    @Override
    public void publishEvent(final EventTranslator<E> translator)
    {
        final long sequence = sequencer.next();
        translateAndPublish(translator, sequence);
    }

    /**
     * @see com.lmax.disruptor.EventSink#tryPublishEvent(com.lmax.disruptor.EventTranslator)
     *
     * 尝试发布单个事件；容量不足时返回 false 而不是阻塞。
     */
    @Override
    public boolean tryPublishEvent(final EventTranslator<E> translator)
    {
        try
        {
            final long sequence = sequencer.tryNext();
            translateAndPublish(translator, sequence);
            return true;
        }
        catch (InsufficientCapacityException e)
        {
            return false;
        }
    }

    /**
     * @see com.lmax.disruptor.EventSink#publishEvent(com.lmax.disruptor.EventTranslatorOneArg, Object)
     * com.lmax.disruptor.EventSink#publishEvent(com.lmax.disruptor.EventTranslatorOneArg, A)
     *
     * 发布单个事件：申请下一个序号后，用携带一个参数的 translator 填充事件并发布。
     */
    @Override
    public <A> void publishEvent(final EventTranslatorOneArg<E, A> translator, final A arg0)
    {
        final long sequence = sequencer.next();
        translateAndPublish(translator, sequence, arg0);
    }

    /**
     * @see com.lmax.disruptor.EventSink#tryPublishEvent(com.lmax.disruptor.EventTranslatorOneArg, Object)
     * com.lmax.disruptor.EventSink#tryPublishEvent(com.lmax.disruptor.EventTranslatorOneArg, A)
     *
     * 尝试用携带一个参数的 translator 发布单个事件；容量不足时返回 false 而不是阻塞。
     */
    @Override
    public <A> boolean tryPublishEvent(final EventTranslatorOneArg<E, A> translator, final A arg0)
    {
        try
        {
            final long sequence = sequencer.tryNext();
            translateAndPublish(translator, sequence, arg0);
            return true;
        }
        catch (InsufficientCapacityException e)
        {
            return false;
        }
    }

    /**
     * @see com.lmax.disruptor.EventSink#publishEvent(com.lmax.disruptor.EventTranslatorTwoArg, Object, Object)
     * com.lmax.disruptor.EventSink#publishEvent(com.lmax.disruptor.EventTranslatorTwoArg, A, B)
     *
     * 发布单个事件：申请下一个序号后，用携带两个参数的 translator 填充事件并发布。
     */
    @Override
    public <A, B> void publishEvent(final EventTranslatorTwoArg<E, A, B> translator, final A arg0, final B arg1)
    {
        final long sequence = sequencer.next();
        translateAndPublish(translator, sequence, arg0, arg1);
    }

    /**
     * @see com.lmax.disruptor.EventSink#tryPublishEvent(com.lmax.disruptor.EventTranslatorTwoArg, Object, Object)
     * com.lmax.disruptor.EventSink#tryPublishEvent(com.lmax.disruptor.EventTranslatorTwoArg, A, B)
     *
     * 尝试用携带两个参数的 translator 发布单个事件；容量不足时返回 false 而不是阻塞。
     */
    @Override
    public <A, B> boolean tryPublishEvent(final EventTranslatorTwoArg<E, A, B> translator, final A arg0, final B arg1)
    {
        try
        {
            final long sequence = sequencer.tryNext();
            translateAndPublish(translator, sequence, arg0, arg1);
            return true;
        }
        catch (InsufficientCapacityException e)
        {
            return false;
        }
    }

    /**
     * @see com.lmax.disruptor.EventSink#publishEvent(com.lmax.disruptor.EventTranslatorThreeArg, Object, Object, Object)
     * com.lmax.disruptor.EventSink#publishEvent(com.lmax.disruptor.EventTranslatorThreeArg, A, B, C)
     *
     * 发布单个事件：申请下一个序号后，用携带三个参数的 translator 填充事件并发布。
     */
    @Override
    public <A, B, C> void publishEvent(final EventTranslatorThreeArg<E, A, B, C> translator, final A arg0, final B arg1, final C arg2)
    {
        final long sequence = sequencer.next();
        translateAndPublish(translator, sequence, arg0, arg1, arg2);
    }

    /**
     * @see com.lmax.disruptor.EventSink#tryPublishEvent(com.lmax.disruptor.EventTranslatorThreeArg, Object, Object, Object)
     * com.lmax.disruptor.EventSink#tryPublishEvent(com.lmax.disruptor.EventTranslatorThreeArg, A, B, C)
     *
     * 尝试用携带三个参数的 translator 发布单个事件；容量不足时返回 false 而不是阻塞。
     */
    @Override
    public <A, B, C> boolean tryPublishEvent(final EventTranslatorThreeArg<E, A, B, C> translator, final A arg0, final B arg1, final C arg2)
    {
        try
        {
            final long sequence = sequencer.tryNext();
            translateAndPublish(translator, sequence, arg0, arg1, arg2);
            return true;
        }
        catch (InsufficientCapacityException e)
        {
            return false;
        }
    }

    /**
     * @see com.lmax.disruptor.EventSink#publishEvent(com.lmax.disruptor.EventTranslatorVararg, java.lang.Object...)
     *
     * 发布单个事件：申请下一个序号后，用可变参数的 translator 填充事件并发布。
     */
    @Override
    public void publishEvent(final EventTranslatorVararg<E> translator, final Object... args)
    {
        final long sequence = sequencer.next();
        translateAndPublish(translator, sequence, args);
    }

    /**
     * @see com.lmax.disruptor.EventSink#tryPublishEvent(com.lmax.disruptor.EventTranslatorVararg, java.lang.Object...)
     *
     * 尝试用可变参数的 translator 发布单个事件；容量不足时返回 false 而不是阻塞。
     */
    @Override
    public boolean tryPublishEvent(final EventTranslatorVararg<E> translator, final Object... args)
    {
        try
        {
            final long sequence = sequencer.tryNext();
            translateAndPublish(translator, sequence, args);
            return true;
        }
        catch (InsufficientCapacityException e)
        {
            return false;
        }
    }


    /**
     * @see com.lmax.disruptor.EventSink#publishEvents(com.lmax.disruptor.EventTranslator[])
     *
     * 批量发布事件：为每个 translator 申请序号、填充事件并一次性发布。
     */
    @Override
    public void publishEvents(final EventTranslator<E>[] translators)
    {
        publishEvents(translators, 0, translators.length);
    }

    /**
     * @see com.lmax.disruptor.EventSink#publishEvents(com.lmax.disruptor.EventTranslator[], int, int)
     *
     * 批量发布事件：从 batchStartsAt 开始取 batchSize 个 translator，填充事件并一次性发布。
     */
    @Override
    public void publishEvents(final EventTranslator<E>[] translators, final int batchStartsAt, final int batchSize)
    {
        checkBounds(translators, batchStartsAt, batchSize);
        final long finalSequence = sequencer.next(batchSize);
        translateAndPublishBatch(translators, batchStartsAt, batchSize, finalSequence);
    }

    /**
     * @see com.lmax.disruptor.EventSink#tryPublishEvents(com.lmax.disruptor.EventTranslator[])
     *
     * 尝试批量发布事件；容量不足时返回 false 而不是阻塞。
     */
    @Override
    public boolean tryPublishEvents(final EventTranslator<E>[] translators)
    {
        return tryPublishEvents(translators, 0, translators.length);
    }

    /**
     * @see com.lmax.disruptor.EventSink#tryPublishEvents(com.lmax.disruptor.EventTranslator[], int, int)
     *
     * 尝试从 batchStartsAt 开始批量发布 batchSize 个事件；容量不足时返回 false 而不是阻塞。
     */
    @Override
    public boolean tryPublishEvents(final EventTranslator<E>[] translators, final int batchStartsAt, final int batchSize)
    {
        checkBounds(translators, batchStartsAt, batchSize);
        try
        {
            final long finalSequence = sequencer.tryNext(batchSize);
            translateAndPublishBatch(translators, batchStartsAt, batchSize, finalSequence);
            return true;
        }
        catch (InsufficientCapacityException e)
        {
            return false;
        }
    }

    /**
     * @see com.lmax.disruptor.EventSink#publishEvents(com.lmax.disruptor.EventTranslatorOneArg, Object[])
     * com.lmax.disruptor.EventSink#publishEvents(com.lmax.disruptor.EventTranslatorOneArg, A[])
     *
     * 批量发布事件：用携带一个参数的 translator 依次为每个事件填充对应参数并一次性发布。
     */
    @Override
    public <A> void publishEvents(final EventTranslatorOneArg<E, A> translator, final A[] arg0)
    {
        publishEvents(translator, 0, arg0.length, arg0);
    }

    /**
     * @see com.lmax.disruptor.EventSink#publishEvents(com.lmax.disruptor.EventTranslatorOneArg, int, int, Object[])
     * com.lmax.disruptor.EventSink#publishEvents(com.lmax.disruptor.EventTranslatorOneArg, int, int, A[])
     *
     * 批量发布事件：从 batchStartsAt 开始取 batchSize 个参数，用携带一个参数的 translator 填充并一次性发布。
     */
    @Override
    public <A> void publishEvents(final EventTranslatorOneArg<E, A> translator, final int batchStartsAt, final int batchSize, final A[] arg0)
    {
        checkBounds(arg0, batchStartsAt, batchSize);
        final long finalSequence = sequencer.next(batchSize);
        translateAndPublishBatch(translator, arg0, batchStartsAt, batchSize, finalSequence);
    }

    /**
     * @see com.lmax.disruptor.EventSink#tryPublishEvents(com.lmax.disruptor.EventTranslatorOneArg, Object[])
     * com.lmax.disruptor.EventSink#tryPublishEvents(com.lmax.disruptor.EventTranslatorOneArg, A[])
     *
     * 尝试用携带一个参数的 translator 批量发布事件；容量不足时返回 false 而不是阻塞。
     */
    @Override
    public <A> boolean tryPublishEvents(final EventTranslatorOneArg<E, A> translator, final A[] arg0)
    {
        return tryPublishEvents(translator, 0, arg0.length, arg0);
    }

    /**
     * @see com.lmax.disruptor.EventSink#tryPublishEvents(com.lmax.disruptor.EventTranslatorOneArg, int, int, Object[])
     * com.lmax.disruptor.EventSink#tryPublishEvents(com.lmax.disruptor.EventTranslatorOneArg, int, int, A[])
     *
     * 尝试从 batchStartsAt 开始用携带一个参数的 translator 批量发布 batchSize 个事件；容量不足时返回 false。
     */
    @Override
    public <A> boolean tryPublishEvents(
            final EventTranslatorOneArg<E, A> translator, final int batchStartsAt, final int batchSize, final A[] arg0)
    {
        checkBounds(arg0, batchStartsAt, batchSize);
        try
        {
            final long finalSequence = sequencer.tryNext(batchSize);
            translateAndPublishBatch(translator, arg0, batchStartsAt, batchSize, finalSequence);
            return true;
        }
        catch (InsufficientCapacityException e)
        {
            return false;
        }
    }

    /**
     * @see com.lmax.disruptor.EventSink#publishEvents(com.lmax.disruptor.EventTranslatorTwoArg, Object[], Object[])
     * com.lmax.disruptor.EventSink#publishEvents(com.lmax.disruptor.EventTranslatorTwoArg, A[], B[])
     *
     * 批量发布事件：用携带两个参数的 translator 依次为每个事件填充对应参数并一次性发布。
     */
    @Override
    public <A, B> void publishEvents(final EventTranslatorTwoArg<E, A, B> translator, final A[] arg0, final B[] arg1)
    {
        publishEvents(translator, 0, arg0.length, arg0, arg1);
    }

    /**
     * @see com.lmax.disruptor.EventSink#publishEvents(com.lmax.disruptor.EventTranslatorTwoArg, int, int, Object[], Object[])
     * com.lmax.disruptor.EventSink#publishEvents(com.lmax.disruptor.EventTranslatorTwoArg, int, int, A[], B[])
     *
     * 批量发布事件：从 batchStartsAt 开始取 batchSize 组参数，用携带两个参数的 translator 填充并一次性发布。
     */
    @Override
    public <A, B> void publishEvents(
        final EventTranslatorTwoArg<E, A, B> translator, final int batchStartsAt, final int batchSize, final A[] arg0, final B[] arg1)
    {
        checkBounds(arg0, arg1, batchStartsAt, batchSize);
        final long finalSequence = sequencer.next(batchSize);
        translateAndPublishBatch(translator, arg0, arg1, batchStartsAt, batchSize, finalSequence);
    }

    /**
     * @see com.lmax.disruptor.EventSink#tryPublishEvents(com.lmax.disruptor.EventTranslatorTwoArg, Object[], Object[])
     * com.lmax.disruptor.EventSink#tryPublishEvents(com.lmax.disruptor.EventTranslatorTwoArg, A[], B[])
     *
     * 尝试用携带两个参数的 translator 批量发布事件；容量不足时返回 false 而不是阻塞。
     */
    @Override
    public <A, B> boolean tryPublishEvents(final EventTranslatorTwoArg<E, A, B> translator, final A[] arg0, final B[] arg1)
    {
        return tryPublishEvents(translator, 0, arg0.length, arg0, arg1);
    }

    /**
     * @see com.lmax.disruptor.EventSink#tryPublishEvents(com.lmax.disruptor.EventTranslatorTwoArg, int, int, Object[], Object[])
     * com.lmax.disruptor.EventSink#tryPublishEvents(com.lmax.disruptor.EventTranslatorTwoArg, int, int, A[], B[])
     *
     * 尝试从 batchStartsAt 开始用携带两个参数的 translator 批量发布 batchSize 个事件；容量不足时返回 false。
     */
    @Override
    public <A, B> boolean tryPublishEvents(
        final EventTranslatorTwoArg<E, A, B> translator, final int batchStartsAt, final int batchSize, final A[] arg0, final B[] arg1)
    {
        checkBounds(arg0, arg1, batchStartsAt, batchSize);
        try
        {
            final long finalSequence = sequencer.tryNext(batchSize);
            translateAndPublishBatch(translator, arg0, arg1, batchStartsAt, batchSize, finalSequence);
            return true;
        }
        catch (InsufficientCapacityException e)
        {
            return false;
        }
    }

    /**
     * @see com.lmax.disruptor.EventSink#publishEvents(com.lmax.disruptor.EventTranslatorThreeArg, Object[], Object[], Object[])
     * com.lmax.disruptor.EventSink#publishEvents(com.lmax.disruptor.EventTranslatorThreeArg, A[], B[], C[])
     *
     * 批量发布事件：用携带三个参数的 translator 依次为每个事件填充对应参数并一次性发布。
     */
    @Override
    public <A, B, C> void publishEvents(final EventTranslatorThreeArg<E, A, B, C> translator, final A[] arg0, final B[] arg1, final C[] arg2)
    {
        publishEvents(translator, 0, arg0.length, arg0, arg1, arg2);
    }

    /**
     * @see com.lmax.disruptor.EventSink#publishEvents(com.lmax.disruptor.EventTranslatorThreeArg, int, int, Object[], Object[], Object[])
     * com.lmax.disruptor.EventSink#publishEvents(com.lmax.disruptor.EventTranslatorThreeArg, int, int, A[], B[], C[])
     *
     * 批量发布事件：从 batchStartsAt 开始取 batchSize 组参数，用携带三个参数的 translator 填充并一次性发布。
     */
    @Override
    public <A, B, C> void publishEvents(
        final EventTranslatorThreeArg<E, A, B, C> translator, final int batchStartsAt, final int batchSize, final A[] arg0, final B[] arg1, final C[] arg2)
    {
        checkBounds(arg0, arg1, arg2, batchStartsAt, batchSize);
        final long finalSequence = sequencer.next(batchSize);
        translateAndPublishBatch(translator, arg0, arg1, arg2, batchStartsAt, batchSize, finalSequence);
    }

    /**
     * @see com.lmax.disruptor.EventSink#tryPublishEvents(com.lmax.disruptor.EventTranslatorThreeArg, Object[], Object[], Object[])
     * com.lmax.disruptor.EventSink#tryPublishEvents(com.lmax.disruptor.EventTranslatorThreeArg, A[], B[], C[])
     *
     * 尝试用携带三个参数的 translator 批量发布事件；容量不足时返回 false 而不是阻塞。
     */
    @Override
    public <A, B, C> boolean tryPublishEvents(
        final EventTranslatorThreeArg<E, A, B, C> translator, final A[] arg0, final B[] arg1, final C[] arg2)
    {
        return tryPublishEvents(translator, 0, arg0.length, arg0, arg1, arg2);
    }

    /**
     * @see com.lmax.disruptor.EventSink#tryPublishEvents(com.lmax.disruptor.EventTranslatorThreeArg, int, int, Object[], Object[], Object[])
     * com.lmax.disruptor.EventSink#tryPublishEvents(com.lmax.disruptor.EventTranslatorThreeArg, int, int, A[], B[], C[])
     *
     * 尝试从 batchStartsAt 开始用携带三个参数的 translator 批量发布 batchSize 个事件；容量不足时返回 false。
     */
    @Override
    public <A, B, C> boolean tryPublishEvents(
        final EventTranslatorThreeArg<E, A, B, C> translator, final int batchStartsAt, final int batchSize, final A[] arg0, final B[] arg1, final C[] arg2)
    {
        checkBounds(arg0, arg1, arg2, batchStartsAt, batchSize);
        try
        {
            final long finalSequence = sequencer.tryNext(batchSize);
            translateAndPublishBatch(translator, arg0, arg1, arg2, batchStartsAt, batchSize, finalSequence);
            return true;
        }
        catch (InsufficientCapacityException e)
        {
            return false;
        }
    }

    /**
     * @see com.lmax.disruptor.EventSink#publishEvents(com.lmax.disruptor.EventTranslatorVararg, java.lang.Object[][])
     *
     * 批量发布事件：用可变参数的 translator 依次为每个事件填充对应参数并一次性发布。
     */
    @Override
    public void publishEvents(final EventTranslatorVararg<E> translator, final Object[]... args)
    {
        publishEvents(translator, 0, args.length, args);
    }

    /**
     * @see com.lmax.disruptor.EventSink#publishEvents(com.lmax.disruptor.EventTranslatorVararg, int, int, java.lang.Object[][])
     *
     * 批量发布事件：从 batchStartsAt 开始取 batchSize 组参数，用可变参数的 translator 填充并一次性发布。
     */
    @Override
    public void publishEvents(final EventTranslatorVararg<E> translator, final int batchStartsAt, final int batchSize, final Object[]... args)
    {
        checkBounds(batchStartsAt, batchSize, args);
        final long finalSequence = sequencer.next(batchSize);
        translateAndPublishBatch(translator, batchStartsAt, batchSize, finalSequence, args);
    }

    /**
     * @see com.lmax.disruptor.EventSink#tryPublishEvents(com.lmax.disruptor.EventTranslatorVararg, java.lang.Object[][])
     *
     * 尝试用可变参数的 translator 批量发布事件；容量不足时返回 false 而不是阻塞。
     */
    @Override
    public boolean tryPublishEvents(final EventTranslatorVararg<E> translator, final Object[]... args)
    {
        return tryPublishEvents(translator, 0, args.length, args);
    }

    /**
     * @see com.lmax.disruptor.EventSink#tryPublishEvents(com.lmax.disruptor.EventTranslatorVararg, int, int, java.lang.Object[][])
     *
     * 尝试从 batchStartsAt 开始用可变参数的 translator 批量发布 batchSize 个事件；容量不足时返回 false。
     */
    @Override
    public boolean tryPublishEvents(
        final EventTranslatorVararg<E> translator, final int batchStartsAt, final int batchSize, final Object[]... args)
    {
        checkBounds(args, batchStartsAt, batchSize);
        try
        {
            final long finalSequence = sequencer.tryNext(batchSize);
            translateAndPublishBatch(translator, batchStartsAt, batchSize, finalSequence, args);
            return true;
        }
        catch (InsufficientCapacityException e)
        {
            return false;
        }
    }

    /**
     * Publish the specified sequence.  This action marks this particular
     * message as being available to be read.
     *
     * 发布指定的序号。该动作将对应的消息标记为可读。
     *
     * @param sequence the sequence to publish.
     *
     * sequence：要发布的序号。
     */
    @Override
    public void publish(final long sequence)
    {
        sequencer.publish(sequence);
    }

    /**
     * Publish the specified sequences.  This action marks these particular
     * messages as being available to be read.
     *
     * 发布指定的序号区间。该动作将这些消息标记为可读。
     *
     * @param lo the lowest sequence number to be published
     * @param hi the highest sequence number to be published
     * @see Sequencer#next(int)
     *
     * lo：要发布的最低序号；hi：要发布的最高序号。
     */
    @Override
    public void publish(final long lo, final long hi)
    {
        sequencer.publish(lo, hi);
    }

    /**
     * Get the remaining capacity for this ringBuffer.
     *
     * 获取本 ringBuffer 的剩余容量。
     *
     * @return The number of slots remaining.
     *
     * 返回剩余槽位数。
     */
    @Override
    public long remainingCapacity()
    {
        return sequencer.remainingCapacity();
    }

    /**
     * Validates the batch window against the translator array before publishing.
     *
     * 在发布前校验批次窗口与 translator 数组是否越界。
     */
    private void checkBounds(final EventTranslator<E>[] translators, final int batchStartsAt, final int batchSize)
    {
        checkBatchSizing(batchStartsAt, batchSize);
        batchOverRuns(translators, batchStartsAt, batchSize);
    }

    /**
     * Checks that the batch start and size are non-negative and fit in the buffer.
     *
     * 检查批次起点和大小非负，且不超过缓冲区容量。
     */
    private void checkBatchSizing(final int batchStartsAt, final int batchSize)
    {
        if (batchStartsAt < 0 || batchSize < 0)
        {
            throw new IllegalArgumentException("Both batchStartsAt and batchSize must be positive but got: batchStartsAt " + batchStartsAt + " and batchSize " + batchSize);
        }
        else if (batchSize > bufferSize)
        {
            throw new IllegalArgumentException("The ring buffer cannot accommodate " + batchSize + " it only has space for " + bufferSize + " entities.");
        }
    }

    /**
     * Validates the batch window against a single argument array.
     *
     * 校验批次窗口与单个参数数组是否越界。
     */
    private <A> void checkBounds(final A[] arg0, final int batchStartsAt, final int batchSize)
    {
        checkBatchSizing(batchStartsAt, batchSize);
        batchOverRuns(arg0, batchStartsAt, batchSize);
    }

    /**
     * Validates the batch window against two argument arrays.
     *
     * 校验批次窗口与两个参数数组是否越界。
     */
    private <A, B> void checkBounds(final A[] arg0, final B[] arg1, final int batchStartsAt, final int batchSize)
    {
        checkBatchSizing(batchStartsAt, batchSize);
        batchOverRuns(arg0, batchStartsAt, batchSize);
        batchOverRuns(arg1, batchStartsAt, batchSize);
    }

    /**
     * Validates the batch window against three argument arrays.
     *
     * 校验批次窗口与三个参数数组是否越界。
     */
    private <A, B, C> void checkBounds(
        final A[] arg0, final B[] arg1, final C[] arg2, final int batchStartsAt, final int batchSize)
    {
        checkBatchSizing(batchStartsAt, batchSize);
        batchOverRuns(arg0, batchStartsAt, batchSize);
        batchOverRuns(arg1, batchStartsAt, batchSize);
        batchOverRuns(arg2, batchStartsAt, batchSize);
    }

    /**
     * Validates the batch window against the vararg argument array.
     *
     * 校验批次窗口与可变参数数组是否越界。
     */
    private void checkBounds(final int batchStartsAt, final int batchSize, final Object[][] args)
    {
        checkBatchSizing(batchStartsAt, batchSize);
        batchOverRuns(args, batchStartsAt, batchSize);
    }

    /**
     * Ensures the batch window does not run past the end of the argument array.
     *
     * 确保批次窗口不会越过参数数组的末尾。
     */
    private <A> void batchOverRuns(final A[] arg0, final int batchStartsAt, final int batchSize)
    {
        if (batchStartsAt + batchSize > arg0.length)
        {
            throw new IllegalArgumentException(
                "A batchSize of: " + batchSize +
                    " with batchStatsAt of: " + batchStartsAt +
                    " will overrun the available number of arguments: " + (arg0.length - batchStartsAt));
        }
    }

    /**
     * Translates the event at the claimed sequence and always publishes it.
     *
     * 填充已申请序号处的事件，并保证无论是否抛异常都会发布该序号。
     */
    private void translateAndPublish(final EventTranslator<E> translator, final long sequence)
    {
        try
        {
            // 用 translator 将数据填充到预分配事件中
            translator.translateTo(get(sequence), sequence);
        }
        finally
        {
            // 即使填充抛出异常也必须发布序号，避免消费者永久等待
            sequencer.publish(sequence);
        }
    }

    /**
     * Translates the event with one argument and always publishes the sequence.
     *
     * 用携带一个参数的 translator 填充事件，并保证最终发布该序号。
     */
    private <A> void translateAndPublish(final EventTranslatorOneArg<E, A> translator, final long sequence, final A arg0)
    {
        try
        {
            // 用 translator 将数据填充到预分配事件中
            translator.translateTo(get(sequence), sequence, arg0);
        }
        finally
        {
            // 即使填充抛出异常也必须发布序号，避免消费者永久等待
            sequencer.publish(sequence);
        }
    }

    /**
     * Translates the event with two arguments and always publishes the sequence.
     *
     * 用携带两个参数的 translator 填充事件，并保证最终发布该序号。
     */
    private <A, B> void translateAndPublish(final EventTranslatorTwoArg<E, A, B> translator, final long sequence, final A arg0, final B arg1)
    {
        try
        {
            // 用 translator 将数据填充到预分配事件中
            translator.translateTo(get(sequence), sequence, arg0, arg1);
        }
        finally
        {
            // 即使填充抛出异常也必须发布序号，避免消费者永久等待
            sequencer.publish(sequence);
        }
    }

    /**
     * Translates the event with three arguments and always publishes the sequence.
     *
     * 用携带三个参数的 translator 填充事件，并保证最终发布该序号。
     */
    private <A, B, C> void translateAndPublish(
        final EventTranslatorThreeArg<E, A, B, C> translator, final long sequence,
        final A arg0, final B arg1, final C arg2)
    {
        try
        {
            // 用 translator 将数据填充到预分配事件中
            translator.translateTo(get(sequence), sequence, arg0, arg1, arg2);
        }
        finally
        {
            // 即使填充抛出异常也必须发布序号，避免消费者永久等待
            sequencer.publish(sequence);
        }
    }

    /**
     * Translates the event with a vararg translator and always publishes the sequence.
     *
     * 用可变参数的 translator 填充事件，并保证最终发布该序号。
     */
    private void translateAndPublish(final EventTranslatorVararg<E> translator, final long sequence, final Object... args)
    {
        try
        {
            // 用 translator 将数据填充到预分配事件中
            translator.translateTo(get(sequence), sequence, args);
        }
        finally
        {
            // 即使填充抛出异常也必须发布序号，避免消费者永久等待
            sequencer.publish(sequence);
        }
    }

    /**
     * Translates a batch of events, one translator per event, then publishes the whole batch.
     *
     * 批量填充事件（每个事件对应一个 translator），然后整体发布整个批次。
     */
    private void translateAndPublishBatch(
        final EventTranslator<E>[] translators, final int batchStartsAt,
        final int batchSize, final long finalSequence)
    {
        // 由批次末尾序号反推批次起始序号
        final long initialSequence = finalSequence - (batchSize - 1);
        try
        {
            long sequence = initialSequence;
            final int batchEndsAt = batchStartsAt + batchSize;
            // 依次用对应的 translator 填充批次内的每个事件
            for (int i = batchStartsAt; i < batchEndsAt; i++)
            {
                final EventTranslator<E> translator = translators[i];
                translator.translateTo(get(sequence), sequence++);
            }
        }
        finally
        {
            // 无论填充是否成功，最后都发布整个批次区间
            sequencer.publish(initialSequence, finalSequence);
        }
    }

    /**
     * Translates a batch of events with one argument each, then publishes the whole batch.
     *
     * 用携带一个参数的 translator 批量填充事件，然后整体发布整个批次。
     */
    private <A> void translateAndPublishBatch(
        final EventTranslatorOneArg<E, A> translator, final A[] arg0,
        final int batchStartsAt, final int batchSize, final long finalSequence)
    {
        // 由批次末尾序号反推批次起始序号
        final long initialSequence = finalSequence - (batchSize - 1);
        try
        {
            long sequence = initialSequence;
            final int batchEndsAt = batchStartsAt + batchSize;
            // 依次用 translator 和对应参数填充批次内的每个事件
            for (int i = batchStartsAt; i < batchEndsAt; i++)
            {
                translator.translateTo(get(sequence), sequence++, arg0[i]);
            }
        }
        finally
        {
            // 无论填充是否成功，最后都发布整个批次区间
            sequencer.publish(initialSequence, finalSequence);
        }
    }

    /**
     * Translates a batch of events with two arguments each, then publishes the whole batch.
     *
     * 用携带两个参数的 translator 批量填充事件，然后整体发布整个批次。
     */
    private <A, B> void translateAndPublishBatch(
        final EventTranslatorTwoArg<E, A, B> translator, final A[] arg0,
        final B[] arg1, final int batchStartsAt, final int batchSize,
        final long finalSequence)
    {
        // 由批次末尾序号反推批次起始序号
        final long initialSequence = finalSequence - (batchSize - 1);
        try
        {
            long sequence = initialSequence;
            final int batchEndsAt = batchStartsAt + batchSize;
            // 依次用 translator 和对应参数填充批次内的每个事件
            for (int i = batchStartsAt; i < batchEndsAt; i++)
            {
                translator.translateTo(get(sequence), sequence++, arg0[i], arg1[i]);
            }
        }
        finally
        {
            // 无论填充是否成功，最后都发布整个批次区间
            sequencer.publish(initialSequence, finalSequence);
        }
    }

    /**
     * Translates a batch of events with three arguments each, then publishes the whole batch.
     *
     * 用携带三个参数的 translator 批量填充事件，然后整体发布整个批次。
     */
    private <A, B, C> void translateAndPublishBatch(
        final EventTranslatorThreeArg<E, A, B, C> translator,
        final A[] arg0, final B[] arg1, final C[] arg2, final int batchStartsAt,
        final int batchSize, final long finalSequence)
    {
        // 由批次末尾序号反推批次起始序号
        final long initialSequence = finalSequence - (batchSize - 1);
        try
        {
            long sequence = initialSequence;
            final int batchEndsAt = batchStartsAt + batchSize;
            // 依次用 translator 和对应参数填充批次内的每个事件
            for (int i = batchStartsAt; i < batchEndsAt; i++)
            {
                translator.translateTo(get(sequence), sequence++, arg0[i], arg1[i], arg2[i]);
            }
        }
        finally
        {
            // 无论填充是否成功，最后都发布整个批次区间
            sequencer.publish(initialSequence, finalSequence);
        }
    }

    /**
     * Translates a batch of events with a vararg translator, then publishes the whole batch.
     *
     * 用可变参数的 translator 批量填充事件，然后整体发布整个批次。
     */
    private void translateAndPublishBatch(
        final EventTranslatorVararg<E> translator, final int batchStartsAt,
        final int batchSize, final long finalSequence, final Object[][] args)
    {
        // 由批次末尾序号反推批次起始序号
        final long initialSequence = finalSequence - (batchSize - 1);
        try
        {
            long sequence = initialSequence;
            final int batchEndsAt = batchStartsAt + batchSize;
            // 依次用 translator 和对应参数填充批次内的每个事件
            for (int i = batchStartsAt; i < batchEndsAt; i++)
            {
                translator.translateTo(get(sequence), sequence++, args[i]);
            }
        }
        finally
        {
            // 无论填充是否成功，最后都发布整个批次区间
            sequencer.publish(initialSequence, finalSequence);
        }
    }

    /**
     * Returns a short description of this ring buffer for logging and debugging.
     *
     * 返回本环形缓冲区的简要描述，用于日志和调试。
     */
    @Override
    public String toString()
    {
        return "RingBuffer{" +
            "bufferSize=" + bufferSize +
            ", sequencer=" + sequencer +
            "}";
    }
}
