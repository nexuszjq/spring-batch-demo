package com.example.batchdemo.job;

import org.springframework.batch.item.ItemProcessor;

/**
 * 简单的清洗逻辑示例：
 * 1. 去除首尾空白
 * 2. 将文本统一转成大写，方便后续匹配
 * 实际业务中可在此处扩展复杂校验或字段拆分。
 */
public class DataCleaningProcessor implements ItemProcessor<String, String> {
    @Override
    public String process(String item) {
        if (item == null) {
            return null;
        }
        return item.trim().toUpperCase();
    }
}
