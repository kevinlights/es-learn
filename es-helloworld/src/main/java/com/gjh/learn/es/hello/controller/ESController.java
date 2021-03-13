package com.gjh.learn.es.hello.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

/**
 * created on 2021/3/13
 *
 * @author kevinlights
 */
@RestController
@RequestMapping(value = "es")
public class ESController {

    @GetMapping(value = "test")
    public void test() throws IOException {

    }
}
