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
package org.apache.giraph.graph;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.apache.mahout.math.list.DoubleArrayList;
import org.apache.mahout.math.list.FloatArrayList;
import org.apache.mahout.math.list.LongArrayList;
import org.apache.mahout.math.map.OpenLongFloatHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public abstract class LongDoubleFloatDoubleVertex extends
    MutableVertex<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(LongDoubleFloatDoubleVertex.class);

  private long vertexId;
  private double vertexValue;
  private OpenLongFloatHashMap verticesWithEdgeValues = new OpenLongFloatHashMap();
  private DoubleArrayList messageList = new DoubleArrayList();
  /** If true, do not do anymore computation on this vertex. */
  boolean halt = false;

  @Override
  public void preApplication()
      throws InstantiationException, IllegalAccessException {
    // Do nothing, might be overriden by the user
  }

  @Override
  public void postApplication() {
    // Do nothing, might be overriden by the user
  }

  @Override
  public void preSuperstep() {
    // Do nothing, might be overriden by the user
  }

  @Override
  public void postSuperstep() {
    // Do nothing, might be overriden by the user
  }


  @Override
  public final boolean addEdge(Edge<LongWritable, FloatWritable> edge) {
    edge.setConf(getGraphState().getContext().getConfiguration());
    if (verticesWithEdgeValues.put(edge.getDestVertexId().get(), edge.getEdgeValue().get())) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("addEdge: Vertex=" + vertexId +
                  ": already added an edge value for dest vertex id " +
                  edge.getDestVertexId());
      }
      return false;
    } else {
      return true;
    }
  }

  @Override
  public final void setVertexId(LongWritable vertexId) {
    this.vertexId = vertexId.get();
  }

  @Override
  public final LongWritable getVertexId() {
    return new LongWritable(vertexId); // TODO: possibly not make new objects every time?
  }

  @Override
  public final DoubleWritable getVertexValue() {
    return new DoubleWritable(vertexValue);
  }

  @Override
  public final void setVertexValue(DoubleWritable vertexValue) {
    this.vertexValue = vertexValue.get();
  }

  @Override
  public final SortedMap<LongWritable, Edge<LongWritable, FloatWritable>> getOutEdgeMap() {
    return new LongLongFloatMap(verticesWithEdgeValues);
  }

  @SuppressWarnings("unchecked")
  @Override
  public final void sendMsg(LongWritable id, DoubleWritable msg) {
    if (msg == null) {
      throw new IllegalArgumentException(
          "sendMsg: Cannot send null message to " + id);
    }
    getGraphState().getGraphMapper().getWorkerCommunications().sendMessageReq(id, msg);
  }

  @Override
  public final void sendMsgToAllEdges(DoubleWritable msg) {
    if (msg == null) {
      throw new IllegalArgumentException(
          "sendMsgToAllEdges: Cannot send null message to all edges");
    }
    LongWritable destVertex = new LongWritable();
    for (long destVertexId : verticesWithEdgeValues.keys().elements()) {
      destVertex.set(destVertexId);
      sendMsg(destVertex, msg);
    }
  }
  @Override
  public MutableVertex<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> instantiateVertex() {
    LongDoubleFloatDoubleVertex vertex = (LongDoubleFloatDoubleVertex) BspUtils.createVertex(
            getGraphState().getContext().getConfiguration(), getGraphState());
    return vertex;
  }

  @Override
  public long getNumVertices() {
    return getGraphState().getNumVertices();
  }

  @Override
  public long getNumEdges() {
    return getGraphState().getNumEdges();
  }

  @Override
  public long getSuperstep() {
    return getGraphState().getSuperstep();
  }


  @SuppressWarnings("unchecked")
  @Override
  public void addVertexRequest(MutableVertex<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> vertex)
      throws IOException {
    getGraphState().getGraphMapper().getWorkerCommunications().addVertexReq(vertex);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void removeVertexRequest(LongWritable vertexId) throws IOException {
    getGraphState().getGraphMapper().getWorkerCommunications().removeVertexReq(vertexId);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void addEdgeRequest(LongWritable vertexIndex,
      Edge<LongWritable, FloatWritable> edge) throws IOException {
    getGraphState().getGraphMapper().getWorkerCommunications().addEdgeReq(vertexIndex, edge);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void removeEdgeRequest(LongWritable sourceVertexId,
      LongWritable destVertexId) throws IOException {
    getGraphState().getGraphMapper().getWorkerCommunications().removeEdgeReq(sourceVertexId, destVertexId);
  }

  @Override
  public final void voteToHalt() {
    halt = true;
  }

  @Override
  public final boolean isHalted() {
    return halt;
  }

  @Override
  final public void readFields(DataInput in) throws IOException {
    vertexId = in.readLong();
    vertexValue = in.readDouble();
    long edgeMapSize = in.readLong();
    for (long i = 0; i < edgeMapSize; ++i) {
      long destVertexId = in.readLong();
      float edgeValue = in.readFloat();
      verticesWithEdgeValues.put(destVertexId, edgeValue);
    }
    long msgListSize = in.readLong();
    for (long i = 0; i < msgListSize; ++i) {
      messageList.add(in.readDouble());
    }
    halt = in.readBoolean();
  }

  @Override
  public final void write(DataOutput out) throws IOException {
    out.writeLong(vertexId);
    out.writeDouble(vertexValue);
    out.writeLong(verticesWithEdgeValues.size());
    for(long destVertexId : verticesWithEdgeValues.keys().elements()) {
      float edgeValue = verticesWithEdgeValues.get(destVertexId);
      out.writeLong(destVertexId);
      out.writeFloat(edgeValue);
    }
    out.writeLong(messageList.size());
    for(double msg : messageList.elements()) {
      out.writeDouble(msg);
    }
    out.writeBoolean(halt);
  }

  @Override
  public List<DoubleWritable> getMsgList() {
    final DoubleWritable message = new DoubleWritable();
    return new AbstractList<DoubleWritable>() {
      @Override public DoubleWritable get(int i) {
        message.set(messageList.get(i));
        return message;
      }
      @Override public int size() {
        return messageList.size();
      }
      @Override public boolean add(DoubleWritable dw) {
        messageList.add(dw.get());
        return true;
      }
      @Override public boolean addAll(Collection<? extends DoubleWritable> collection) {
        for(DoubleWritable dw : collection) {
          messageList.add(dw.get());
        }
        return true;
      }
      @Override public void clear() {
        messageList.clear();
      }
      @Override public boolean isEmpty() {
        return messageList.isEmpty();
      }
    };
  }

  @Override
  public String toString() {
    return "Vertex(id=" + getVertexId() + ",value=" + getVertexValue() +
           ",#edges=" + getOutEdgeMap().size() + ")";
  }

  public static final class LongLongFloatMap
      implements SortedMap<LongWritable, Edge<LongWritable, FloatWritable>> {

    private final OpenLongFloatHashMap map;

    public LongLongFloatMap(OpenLongFloatHashMap map) {
      this.map = map;
    }

    @Override public Comparator<? super LongWritable> comparator() {
      throw new UnsupportedOperationException("Not Implemented Yet!");
    }

    @Override public SortedMap<LongWritable, Edge<LongWritable, FloatWritable>> subMap(
        LongWritable longWritable, LongWritable longWritable1) {
      throw new UnsupportedOperationException("Not Implemented Yet!");
    }

    @Override public SortedMap<LongWritable, Edge<LongWritable, FloatWritable>> headMap(
        LongWritable longWritable) {
      throw new UnsupportedOperationException("Not Implemented Yet!");
    }

    @Override public SortedMap<LongWritable, Edge<LongWritable, FloatWritable>> tailMap(
        LongWritable longWritable) {
      throw new UnsupportedOperationException("Not Implemented Yet!");
    }

    @Override public LongWritable firstKey() {
      throw new UnsupportedOperationException("Not Implemented Yet!");
    }

    @Override public LongWritable lastKey() {
      throw new UnsupportedOperationException("Not Implemented Yet!");
    }

    @Override public int size() {
      return map.size();
    }

    @Override public boolean isEmpty() {
      return map.isEmpty();
    }

    @Override public boolean containsKey(Object o) {
      throw new UnsupportedOperationException("Not Implemented Yet!");
    }

    @Override public boolean containsValue(Object o) {
      throw new UnsupportedOperationException("Not Implemented Yet!");
    }

    @Override public Edge<LongWritable, FloatWritable> get(Object o) {
      throw new UnsupportedOperationException("Not Implemented Yet!");
    }

    @Override public Edge<LongWritable, FloatWritable> put(LongWritable longWritable,
        Edge<LongWritable, FloatWritable> edge) {
      throw new UnsupportedOperationException("Not Implemented Yet!");
    }

    @Override public Edge<LongWritable, FloatWritable> remove(Object o) {
      LongWritable edgeId = (LongWritable) o;
      if(map.containsKey(edgeId.get())) {
        Edge<LongWritable, FloatWritable> edge =
            new Edge<LongWritable, FloatWritable>(edgeId, new FloatWritable(map.get(edgeId.get())));
        map.removeKey(edgeId.get());
        return edge;
      } else {
        return null;
      }
    }

    @Override
    public void putAll(Map<? extends LongWritable, ? extends Edge<LongWritable, FloatWritable>> map) {
      throw new UnsupportedOperationException("Not Implemented Yet!");
    }

    @Override public void clear() {
      map.clear();
    }

    @Override public Set<LongWritable> keySet() {
      throw new UnsupportedOperationException("Not Implemented Yet!");
    }

    @Override public Collection<Edge<LongWritable, FloatWritable>> values() {
      final LongArrayList keys = map.keys();
      final FloatArrayList values = map.values();
      return new AbstractList<Edge<LongWritable, FloatWritable>>() {
        @Override public Edge<LongWritable, FloatWritable> get(int i) {
          return new Edge<LongWritable, FloatWritable>(new LongWritable(keys.get(i)),
              new FloatWritable(values.get(i)));
        }
        @Override public int size() {
          return map.size();
        }
      };
    }

    @Override public Set<Entry<LongWritable, Edge<LongWritable, FloatWritable>>> entrySet() {
      throw new UnsupportedOperationException("Not Implemented Yet!");
    }
  }

}
