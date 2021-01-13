package com.oldtan.config.util;

import cn.hutool.db.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.util.ObjectUtils;

import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @Description: Hu tool page change spring jpa page util tools.
 * @Author: tanchuyue
 * @Date: 21-1-13
 */
public class PageUtil {

    /**
     * Hu tools page request change to Spring JPA PageRequest object.
     * set default max page size is 50.
     * @param huPage
     * @return
     */
    public static PageRequest toPageRequest(Page huPage){
        return toPageRequest(huPage, 50);
    }

    /**
     * Hu tools page request change to Spring JPA PageRequest object.
     * @param huPage
     * @param maxPageSize set default max page size
     * @return
     */
    public static PageRequest toPageRequest(Page huPage, int maxPageSize){
        huPage = Stream.of(huPage).filter((p) -> p.getPageNumber() > -1)
                .filter((p) -> p.getPageSize() < maxPageSize).findFirst().orElseGet(Page::new);
        return ObjectUtils.isEmpty(huPage.getOrders()) ? PageRequest.of(huPage.getPageNumber(), huPage.getPageSize()) :
                PageRequest.of(huPage.getPageNumber(), huPage.getPageSize(),
                        Sort.by(Stream.of(huPage.getOrders()).filter(Objects::nonNull)
                                .map(order -> {
                                    switch (order.getDirection()){
                                        case ASC: return Sort.Order.asc(order.getField());
                                        default: return Sort.Order.desc(order.getField());
                                    }
                                }).collect(Collectors.toList())));
    }

}
