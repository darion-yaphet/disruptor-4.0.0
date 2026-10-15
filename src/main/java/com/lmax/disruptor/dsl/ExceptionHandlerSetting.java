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
import com.lmax.disruptor.EventHandlerIdentity;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.ExceptionHandler;

/**
 * A support class used as part of setting an exception handler for a specific event handler.
 * For example:
 * <pre><code>disruptorWizard.handleExceptionsIn(eventHandler).with(exceptionHandler);</code></pre>
 *
 * 为指定事件处理器设置异常处理器的支撑类。用法示例见上面代码。
 *
 * @param <T> the type of event being handled.
 *
 * 参数 <T>：被处理事件的类型。
 */
public class ExceptionHandlerSetting<T>
{
    private final EventHandlerIdentity handlerIdentity;
    private final ConsumerRepository consumerRepository;

    /**
     * Creates the setting for the given handler within the given consumer repository.
     *
     * 为给定消费者仓库中的指定事件处理器创建设置对象。
     */
    ExceptionHandlerSetting(
        final EventHandlerIdentity handlerIdentity,
        final ConsumerRepository consumerRepository)
    {
        this.handlerIdentity = handlerIdentity;
        this.consumerRepository = consumerRepository;
    }

    /**
     * Specify the {@link ExceptionHandler} to use with the event handler.
     *
     * 指定该事件处理器要使用的 ExceptionHandler。
     *
     * @param exceptionHandler the exception handler to use.
     *
     * exceptionHandler：要使用的异常处理器。
     */
    @SuppressWarnings("unchecked")
    public void with(final ExceptionHandler<? super T> exceptionHandler)
    {
        final EventProcessor eventProcessor = consumerRepository.getEventProcessorFor(handlerIdentity);
        // 只有 BatchEventProcessor 支持异常处理器，设置后唤醒其屏障让新处理器生效
        if (eventProcessor instanceof BatchEventProcessor)
        {
            ((BatchEventProcessor<T>) eventProcessor).setExceptionHandler(exceptionHandler);
            consumerRepository.getBarrierFor(handlerIdentity).alert();
        }
        else
        {
            throw new RuntimeException(
                "EventProcessor: " + eventProcessor + " is not a BatchEventProcessor " +
                "and does not support exception handlers");
        }
    }
}
