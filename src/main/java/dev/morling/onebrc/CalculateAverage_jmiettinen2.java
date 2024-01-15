/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public class CalculateAverage_jmiettinen2 {

    private static final String FILE = "./measurements.txt";


    static class Result {

        double min;
        double max;
        double sum;
        long count;

        Result(double min, double max, double sum, long count) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
        }

    }

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        var results = Files.lines(Path.of(FILE))
                .map(l -> l.split(";"))
                .collect(Collectors.toMap(
                        parts -> parts[0],
                        parts -> {
                            double temperature = Double.parseDouble(parts[1]);
                            return new Result(temperature, temperature, temperature, 1);
                        },
                        (oldResult, newResult) -> {
                            oldResult.min = Math.min(oldResult.min, newResult.min);
                            oldResult.max = Math.max(oldResult.max, newResult.max);
                            oldResult.sum = oldResult.sum + newResult.sum;
                            oldResult.count = oldResult.count + newResult.count;
                            return oldResult;
                        }, ConcurrentHashMap::new));
        System.out.println(System.currentTimeMillis() - start);
        var asSorted = new TreeMap<>(results);
        System.out.println(asSorted);
    }
}
