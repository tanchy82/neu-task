package com.oldtan.config;

import lombok.SneakyThrows;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.method.support.HandlerMethodReturnValueHandler;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;
import org.springframework.web.servlet.mvc.method.annotation.RequestResponseBodyMethodProcessor;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description: Global unite response structure body config.
 * @Author: tanchuyue
 * @Date: 21-1-12
 */
@Configuration
public class ResponseBodyWrapFactoryBean implements InitializingBean {

    @Autowired
    private RequestMappingHandlerAdapter requestMappingHandlerAdapter;

    @Override
    @SneakyThrows
    public void afterPropertiesSet() {
        List<HandlerMethodReturnValueHandler> unmodifiableList = requestMappingHandlerAdapter.getReturnValueHandlers();
        List<HandlerMethodReturnValueHandler> list = new ArrayList<>(unmodifiableList.size());
        for (HandlerMethodReturnValueHandler returnValueHandler : unmodifiableList) {
            if (returnValueHandler instanceof RequestResponseBodyMethodProcessor) {
                list.add(new ResponseBodyWrapHandler(returnValueHandler));
            } else {
                list.add(returnValueHandler);
            }
        }
        requestMappingHandlerAdapter.setReturnValueHandlers(list);
    }
}
