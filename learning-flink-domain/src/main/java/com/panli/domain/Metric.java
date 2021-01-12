package com.panli.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

/**
 * @author lipan
 * @date 2021-01-12
 * @desc Metric 实体类
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class Metric {

    private String name;
    private long timestamp;
    private Map<String, Object> fields;
    private Map<String, String> tags;
}
