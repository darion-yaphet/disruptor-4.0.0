/*
 * Copyright 2012 LMAX Ltd.
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

/**
 * Defines producer types to support creation of RingBuffer with correct sequencer and publisher.
 *
 * 定义生产者类型，用于以正确的序号器（sequencer）和发布方式创建 RingBuffer。
 */
public enum ProducerType
{
    /**
     * Create a RingBuffer with a single event publisher to the RingBuffer
     *
     * 创建一个只有单个事件发布者的 RingBuffer
     */
    SINGLE,

    /**
     * Create a RingBuffer supporting multiple event publishers to the one RingBuffer
     *
     * 创建一个支持多个事件发布者同时写入的 RingBuffer
     */
    MULTI
}
