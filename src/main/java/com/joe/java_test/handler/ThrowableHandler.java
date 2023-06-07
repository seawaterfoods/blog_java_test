package com.joe.java_test.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@Slf4j
@ControllerAdvice
public class ThrowableHandler
{
    @ExceptionHandler
    public String errorHandler(Error e){
        String msg = e+" : "+e.getMessage()+"get!!";
        log.error(msg);

        return msg;
    }
}
