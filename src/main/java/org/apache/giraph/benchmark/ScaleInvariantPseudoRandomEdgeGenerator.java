package org.apache.giraph.benchmark;

import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.MutableVertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import java.util.Random;

/**
 *  vertex: a_0 * b^(n-1) + a_1 * b^(n-2) + ... + a_n-1 -> (a_0, ... , a_n-1) all less than b.
 *  { seeds[0][a_0], seeds[1][a_0], ... , seeds[n-1][a_n-1] } is a set of n int[] arrays, with values < b.
 */
public class ScaleInvariantPseudoRandomEdgeGenerator {

    private int[][][] seedAdjacencyMatrices;
    private int numSeeds;
    private int seedSize;
    private long[] bases;

    public ScaleInvariantPseudoRandomEdgeGenerator(int[][] seedAdjacencyMatrix, int numSeeds, int seedSize,
                                                   Random random, float noiseProbability) {
        this.numSeeds = numSeeds;
        this.seedSize = seedSize;
        seedAdjacencyMatrices = new int[numSeeds][][];
        for(int i=0; i<numSeeds; i++) {
            seedAdjacencyMatrices[i] = new int[seedAdjacencyMatrix.length][];
            for(int j=0; j<seedAdjacencyMatrix.length; j++) {
                seedAdjacencyMatrices[i][j] = seedAdjacencyMatrix[j].clone();
                for(int k=0; k<seedAdjacencyMatrices[i][j].length; k++) {
                    if(random.nextFloat() < noiseProbability) {
                        seedAdjacencyMatrices[i][j][k] =
                                (seedAdjacencyMatrices[i][j][k] + random.nextInt(seedSize)) % seedSize;
                    }
                }
            }
        }
        bases = new long[numSeeds];
        bases[0] = 0;
        for(int pow=1; pow<numSeeds; pow++) {
            bases[pow] = (long)Math.pow(seedSize, pow);
        }
    }

    public void addEdges(MutableVertex<LongWritable, DoubleWritable, DoubleWritable, ?> vertex) {
        int[][] nonZeroes = new int[numSeeds][];
        int[] digits = digits(vertex.getVertexId().get());
        for(int seed=0; seed<numSeeds; seed++) {
            nonZeroes[seed] = seedAdjacencyMatrices[seed][digits[seed]];
        }
        addAllCombinations(nonZeroes, 0, numSeeds-1, bases, vertex);
    }

    public static void addAllCombinations(int[][] v, long sum, int depth, long[] bases,
                                          MutableVertex<LongWritable, DoubleWritable, DoubleWritable, ?> vertex) {
        if(depth < 0) {
            vertex.addEdge(new Edge<LongWritable, DoubleWritable>(new LongWritable(sum), new DoubleWritable(1d)));
        } else {
            int[] values = v[depth];
            for(int x : values) {
                addAllCombinations(v, (x * bases[depth]) + sum, depth - 1, bases, vertex);
            }
        }
    }

    public int[] digits(long input) {
        int[] digits = new int[numSeeds];
        for(int pow = 0; pow < numSeeds; pow++) {
            digits[pow] = (int) (input % bases[pow]);
        }
        return digits;
    }
}
