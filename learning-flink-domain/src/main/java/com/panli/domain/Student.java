package com.panli.domain;

import lombok.*;

/**
 * @author lipan
 * @date 2021-01-12
 * @desc 学生实体类
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class Student {

    private int id;
    private String name;
    private String pwd;
    private int age;
}
