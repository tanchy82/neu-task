package com.oldtan.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import javax.servlet.http.HttpServletRequest;
import java.util.Enumeration;

/**
 * @Description: Global Exception handler.
 * @Author: tanchuyue
 * @Date: 21-1-12
 */
@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(value = Exception.class)
    public ResponseEntity exceptionHandler(HttpServletRequest req, Exception e){
        log.error("************************Error log start*******************************");
        log.error(String.format("Error message: %s", e.getMessage()));
        log.error(String.format("Request path: %s", req.getRequestURL()));
        Enumeration enumeration = req.getParameterNames();
        log.error("Request parameter:");
        while (enumeration.hasMoreElements()) {
            String name = enumeration.nextElement().toString();
            log.error(name + "---" + req.getParameter(name));
        }
        StackTraceElement[] error = e == null ? null :e.getStackTrace();
        for (StackTraceElement stackTraceElement : error) {
            log.error(stackTraceElement.toString());
        }
        log.error("************************Exception log end*******************************");
        return ResponseEntity.ok().body(ResponseEntity.badRequest().body(e.getMessage()));
    }

}
