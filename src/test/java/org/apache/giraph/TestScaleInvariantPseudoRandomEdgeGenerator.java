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
 import org.apache.giraph.graph.GraphMapper;
 import org.apache.giraph.graph.GraphState;
 import org.apache.giraph.graph.MutableVertex;
 import org.apache.giraph.graph.Vertex;
 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.io.DoubleWritable;
 import org.apache.hadoop.io.LongWritable;
 import org.apache.hadoop.io.Writable;
 import org.apache.hadoop.mapreduce.Mapper;
 import org.mockito.Mockito;

 import java.io.IOException;
 import java.util.ArrayList;
 import java.util.List;
 import java.util.Random;

public class TestScaleInvariantPseudoRandomEdgeGenerator extends TestCase {

    private ScaleInvariantPseudoRandomEdgeGenerator generator;

    @Override
    public void setUp() {
        int[][] seed = new int[][] {{0,1,2}, {0,1,5}, {1,3,5}, {0,4,6,7}, {1,2}, {5,6,7}, {0, 3, 5}, {6}};
        generator = new ScaleInvariantPseudoRandomEdgeGenerator(seed, 4, seed.length, new Random(1234), 0.1f);
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
        int[] logHistogram = new int[32];
        for(MutableVertex<LongWritable, DoubleWritable, DoubleWritable, ?> vertex : vertexes) {
            int logNumEdges = (int) Math.log(vertex.getOutEdgeMap().size()+1);
            logHistogram[logNumEdges]++;
        }
        for(int h : logHistogram) {
            StringBuffer buffer = new StringBuffer();
            while(h>0) {
                buffer.append("*");
                h--;
            }
            System.out.println(buffer.toString());
        }
    }

    private static class ContextWrapper
            extends GraphMapper<LongWritable, DoubleWritable, DoubleWritable, Writable> {
        public Context getContext(Configuration configuration) throws IOException, InterruptedException {
            return new Context(configuration, null, null, null, null, null, null);
        }
    }
}
