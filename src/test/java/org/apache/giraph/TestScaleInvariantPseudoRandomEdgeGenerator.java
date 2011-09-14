 /*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph;

 import junit.framework.TestCase;
import org.apache.giraph.benchmark.PageRankBenchmark;
import org.apache.giraph.benchmark.ScaleInvariantPseudoRandomEdgeGenerator;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.MutableVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestScaleInvariantPseudoRandomEdgeGenerator extends TestCase {

    private int[][] seed;
    private ScaleInvariantPseudoRandomEdgeGenerator generator;

    @Override
    public void setUp() {
        seed = new int[][] {{0,2}, {0,1}, {1}, {}, {}, {5}, {0,5}, {6}};
        generator = new ScaleInvariantPseudoRandomEdgeGenerator(seed, 8, seed.length, new Random(1234), 0.1f);
    }

    public void testBuildGraph() throws Exception {
        List<MutableVertex<LongWritable, DoubleWritable, DoubleWritable, ?>> vertexes =
                new ArrayList<MutableVertex<LongWritable, DoubleWritable, DoubleWritable, ?>>();
        long numVertices = 1000;
        Configuration conf = new Configuration();
        conf.set(GiraphJob.VERTEX_CLASS, PageRankBenchmark.class.getName());
        GraphState<LongWritable, DoubleWritable, DoubleWritable, ?> state =
                new GraphState<LongWritable, DoubleWritable, DoubleWritable, Writable>();
        state.setContext(Mockito.mock(Mapper.Context.class));
        state.setNumVertices(numVertices);
        for(long vertexId = 0; vertexId < numVertices; vertexId++) {
            Vertex<LongWritable, DoubleWritable, DoubleWritable, ?> vertex =
                    BspUtils.createVertex(conf, state);
            vertex.setVertexId(new LongWritable(vertexId));
            vertex.setVertexValue(new DoubleWritable(1d));
            generator.addEdges(vertex);
            vertexes.add(vertex);
        }
        double[] logHistogram = new double[32];
        for(MutableVertex<LongWritable, DoubleWritable, DoubleWritable, ?> vertex : vertexes) {
            int n = vertex.getOutEdgeMap().size() + 1;
            int logNumEdges = (int) (Math.log(n) / Math.log(2));
            logHistogram[logNumEdges]++;
        }
        for(int i=0; i<logHistogram.length; i++) {
            logHistogram[i] = Math.log(logHistogram[i] + 1) / Math.log(2);
        }
        for(double h : logHistogram) {
            StringBuffer buffer = new StringBuffer();
            while(h>0) {
                buffer.append("*");
                h--;
            }
            System.out.println(buffer.toString());
        }
    }

    public void testDigitifier() throws Exception {
        int base = 10;
        int originalNumber = 12345;
        generator = new ScaleInvariantPseudoRandomEdgeGenerator(seed, seed.length, base, new Random(1234), 0f);
        int[] digits = generator.digits(originalNumber);
        int sum = 0;
        for(int i=0; i<digits.length; i++) {
            sum += digits[i] * Math.pow(base, i);
        }
        assertEquals("Digitifier not working!", sum, originalNumber);
    }
}
