package com.jimo.ch.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;

/**
 * @author jimo
 * @version 1.0.0
 * @date 2020/8/29 16:34
 */
public class LogProxy<T> implements InvocationHandler {

    public static final Logger log = LoggerFactory.getLogger(LogProxy.class);

    private final T object;
    private final Class<T> clazz;

    public LogProxy(T object, Class<T> interfaceClass) {
        if (!interfaceClass.isInterface()) {
            throw new IllegalStateException("Class " + interfaceClass.getName() + " is not an interface");
        }
        this.clazz = interfaceClass;
        this.object = object;
    }

    public static <T> T wrap(Class<T> interfaceClass, T object) {
        if (log.isTraceEnabled()) {
            LogProxy<T> proxy = new LogProxy<>(object, interfaceClass);
            return proxy.getProxy();
        }
        return object;
    }

    @SuppressWarnings("unchecked")
    public T getProxy() {
        return (T) Proxy.newProxyInstance(clazz.getClassLoader(), new Class<?>[]{clazz}, this);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String msg = String.format("Call class: %s\n Method: %s\n Object: %s\n Args: %s\n Invoke result: ",
                object.getClass().getName(), method.getName(), object, Arrays.toString(args));
        try {
            final Object invokeResult = method.invoke(object, args);
            msg += invokeResult;
            return invokeResult;
        } catch (InvocationTargetException e) {
            msg += e.getMessage();
            throw e.getTargetException();
        } finally {
            msg = "=== ClickHouse JDBC trace begin ===\n" + msg + "\n === ClickHouse JDBC trace end ===";
            log.trace(msg);
        }
    }
}
