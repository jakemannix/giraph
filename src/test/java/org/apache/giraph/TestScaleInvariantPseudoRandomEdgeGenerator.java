package org.apache.giraph;

import junit.framework.TestCase;
import org.apache.giraph.benchmark.PageRankBenchmark;
import org.apache.giraph.benchmark.ScaleInvariantPseudoRandomEdgeGenerator;
import org.apache.giraph.graph.MutableVertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import java.util.ArrayList;
import java.util.List;

public class TestScaleInvariantPseudoRandomEdgeGenerator extends TestCase {

    private ScaleInvariantPseudoRandomEdgeGenerator generator;

    public void testBuildGraph() throws Exception {
        List<MutableVertex<LongWritable, DoubleWritable, DoubleWritable, ?>> vertexes =
                new ArrayList<MutableVertex<LongWritable, DoubleWritable, DoubleWritable, ?>>();
        long numVertices = 1000;
        for(long vertexId = 0; vertexId < numVertices; vertexId++) {
            MutableVertex<LongWritable, DoubleWritable, DoubleWritable, ?> vertex =
                    new PageRankBenchmark();
            
            vertex.setVertexId(new LongWritable(vertexId));
            vertex.setVertexValue(new DoubleWritable(1d));
            generator.addEdges(vertex);
        }
    }
}
