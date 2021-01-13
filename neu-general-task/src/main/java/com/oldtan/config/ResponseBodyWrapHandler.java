package com.oldtan.config;

import org.springframework.core.MethodParameter;
import org.springframework.http.ResponseEntity;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodReturnValueHandler;
import org.springframework.web.method.support.ModelAndViewContainer;

/**
 * @Description: Global unite response structure body handler.
 * @Author: tanchuyue
 * @Date: 21-1-12
 */
public class ResponseBodyWrapHandler implements HandlerMethodReturnValueHandler {

    private HandlerMethodReturnValueHandler delegate;

    public  ResponseBodyWrapHandler(HandlerMethodReturnValueHandler delegate){
        this.delegate = delegate;
    }

    @Override
    public boolean supportsReturnType(MethodParameter returnType) {
        return delegate.supportsReturnType(returnType);
    }

    @Override
    public void handleReturnValue(Object returnValue, MethodParameter returnType, ModelAndViewContainer modelAndViewContainer, NativeWebRequest nativeWebRequest) throws Exception {
        ResponseEntity response;
        if (returnValue instanceof ResponseEntity){
            response = (ResponseEntity)returnValue;
        }else {
            response = ResponseEntity.ok().body(returnValue);
        }
        delegate.handleReturnValue(response, returnType, modelAndViewContainer, nativeWebRequest);
    }

    public void setDelegate(HandlerMethodReturnValueHandler delegate) {
        this.delegate = delegate;
    }
}
