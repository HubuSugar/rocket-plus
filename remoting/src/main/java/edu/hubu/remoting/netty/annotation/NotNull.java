package edu.hubu.remoting.netty.annotation;

import java.lang.annotation.*;

/**
 * @author: sugar
 * @date: 2023/6/10
 * @description:
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.LOCAL_VARIABLE})
public @interface NotNull {
}
