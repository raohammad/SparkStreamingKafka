/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.flexilogix;

import org.apache.spark.api.java.function.*;
import scala.Tuple2;

public class TextFilterFunction
        implements Function<Tuple2<Long, String>, Tuple2<Long, String>> {

    private static final long serialVersionUID = 42l;

    @Override
    public Tuple2<Long, String> call(Tuple2<Long, String> tweet) {
        String text = tweet._2();
        text = text.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase();
        return new Tuple2<Long, String>(tweet._1(), text);
    }
}
