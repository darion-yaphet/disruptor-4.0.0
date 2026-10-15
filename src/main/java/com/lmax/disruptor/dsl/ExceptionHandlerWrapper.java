package com.lmax.disruptor.dsl;

import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.ExceptionHandlers;

/**
 * A mutable exception handler wrapper
 *
 * 可变的异常处理器包装器，允许在运行期切换底层异常处理器。
 * @param <T> The data type of the underlying {@link com.lmax.disruptor.RingBuffer}
 *
 * 参数 <T>：底层 RingBuffer 的数据类型。
 */
public class ExceptionHandlerWrapper<T> implements ExceptionHandler<T>
{
    private ExceptionHandler<? super T> delegate;

    /**
     * Switch to a different exception handler
     *
     * 切换到另一个异常处理器
     * @param exceptionHandler the exception handler to use from now on
     *
     * exceptionHandler：从现在开始使用的异常处理器
     */
    public void switchTo(final ExceptionHandler<? super T> exceptionHandler)
    {
        this.delegate = exceptionHandler;
    }

    /**
     * Forwards an event-processing exception to the current delegate.
     *
     * 把事件处理过程中的异常转发给当前的委托处理器。
     */
    @Override
    public void handleEventException(final Throwable ex, final long sequence, final T event)
    {
        getExceptionHandler().handleEventException(ex, sequence, event);
    }

    /**
     * Forwards an on-start exception to the current delegate.
     *
     * 把启动阶段的异常转发给当前的委托处理器。
     */
    @Override
    public void handleOnStartException(final Throwable ex)
    {
        getExceptionHandler().handleOnStartException(ex);
    }

    /**
     * Forwards an on-shutdown exception to the current delegate.
     *
     * 把关闭阶段的异常转发给当前的委托处理器。
     */
    @Override
    public void handleOnShutdownException(final Throwable ex)
    {
        getExceptionHandler() .handleOnShutdownException(ex);
    }

    private ExceptionHandler<? super T> getExceptionHandler()
    {
        ExceptionHandler<? super T> handler = delegate;
        // 未设置委托处理器时回退到默认处理器
        return handler == null ? ExceptionHandlers.defaultHandler() : handler;
    }
}
