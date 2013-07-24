package org.apache.helix.controller.rebalancer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.IdealState.IdealStateModeProperty;
import org.apache.log4j.Logger;

public class AutoRebalancer implements Rebalancer {
  HelixManager manager;
  AutoRebalanceModeAlgorithm algorithm;
  
  @Override
  public void init(HelixManager manager)
  {
    this.manager = manager;
    this.algorithm = new AutoRebalanceModeAlgorithm();
  }

  @Override
  public IdealState computeNewIdealState(String resourceName,
      IdealState currentIdealState, CurrentStateOutput currentStateOutput,
      ClusterDataCache clusterData)
  {
    List<String> partitions = new ArrayList<String>(currentIdealState.getPartitionSet());
    String stateModelName = currentIdealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = clusterData.getStateModelDef(stateModelName);
    Map<String, LiveInstance> liveInstance = clusterData.getLiveInstances();
    String replicas = currentIdealState.getReplicas();
    
    LinkedHashMap<String, Integer> stateCountMap = new LinkedHashMap<String, Integer>();
    stateCountMap = stateCount(stateModelDef, liveInstance.size(), Integer.parseInt(replicas));
    List<String> liveNodes = new ArrayList<String>(liveInstance.keySet());
    Map<String, Map<String, String>> currentMapping = currentMapping(currentStateOutput, resourceName, partitions, stateCountMap);
    
    List<String> allNodes = new ArrayList<String>(clusterData.getInstanceConfigMap().keySet());
    ZNRecord newMapping = algorithm.computeNIS(resourceName, partitions,
        stateCountMap, liveNodes, currentMapping, partitions.size(), allNodes);

    IdealState newIdealState = new IdealState(resourceName);
    newIdealState.getRecord().setSimpleFields(currentIdealState.getRecord().getSimpleFields());
    newIdealState.setIdealStateMode(IdealStateModeProperty.AUTO.toString());
    newIdealState.getRecord().setListFields(newMapping.getListFields());
    return newIdealState;
  }
  
  public static class AutoRebalanceModeAlgorithm
  {

    private static Logger logger = Logger.getLogger(AutoRebalancer.class);

    public ZNRecord computeNIS(String resourceName,
        final List<String> partitions,
        final LinkedHashMap<String, Integer> states,
        final List<String> liveNodes,
        final Map<String, Map<String, String>> currentMapping,
        int maximumPerNode, final List<String> allNodes)
    {
      int numReplicas = countStateReplicas(states);
      ZNRecord znRecord = new ZNRecord(resourceName);
      if (liveNodes.size() == 0)
      {
        return znRecord;
      }
      int distRemainder = (numReplicas * partitions.size()) % liveNodes.size();
      int distFloor = (numReplicas * partitions.size()) / liveNodes.size();
      Map<String, Node> nodeMap = new HashMap<String, Node>();
      List<Node> liveNodesList = new ArrayList<Node>();

      for (String id : allNodes)
      {
        Node node = new Node(id);
        node.capacity = 0;
        nodeMap.put(id, node);
      }
      for (int i = 0; i < liveNodes.size(); i++)
      {
        int targetSize = (maximumPerNode > 0) ? Math.min(distFloor,
            maximumPerNode) : distFloor;
        if (distRemainder > 0 && targetSize < maximumPerNode)
        {
          targetSize += 1;
          distRemainder = distRemainder - 1;
        }
        Node node = nodeMap.get(liveNodes.get(i));
        node.isAlive = true;
        node.capacity = targetSize;
        liveNodesList.add(node);
      }
      // compute the preferred mapping if all nodes were up
      Map<Replica, Node> preferredAssignment;
      preferredAssignment = computePreferredPlacement(partitions, states,
          allNodes, nodeMap);
      // logger.info("preferred mapping:"+ preferredAssignment);
      // from current mapping derive the ones in preferred location
      Map<Replica, Node> existingPreferredAssignment;
      existingPreferredAssignment = computeExistingPreferredPlacement(states,
          currentMapping, nodeMap, preferredAssignment);

      // compute orphaned replica that are not assigned to any node
      Set<Replica> orphaned;
      orphaned = computeOrphaned(states, currentMapping, nodeMap,
          preferredAssignment);

      // from current mapping derive the ones not in preferred location
      Map<Replica, Node> existingNonPreferredAssignment;
      existingNonPreferredAssignment = computeExistingNonPreferredPlacement(
          states, currentMapping, nodeMap, preferredAssignment,
          existingPreferredAssignment);

      // iterate through non preferred and see if we can move them to
      // preferredlocation if the donor has more than it should and stealer has
      // enough capacity
      Iterator<Entry<Replica, Node>> iterator = existingNonPreferredAssignment
          .entrySet().iterator();
      while (iterator.hasNext())
      {
        Entry<Replica, Node> entry = iterator.next();
        Replica replica = entry.getKey();
        Node donor = entry.getValue();
        Node receiver = preferredAssignment.get(replica);
        if (donor.capacity < donor.currentlyAssigned
            && receiver.capacity > receiver.currentlyAssigned
            && receiver.canAdd(replica))
        {
          donor.currentlyAssigned = donor.currentlyAssigned - 1;
          receiver.currentlyAssigned = receiver.currentlyAssigned + 1;
          donor.nonPreferred.remove(replica);
          receiver.preferred.add(replica);
          iterator.remove();
        }
      }
      // assign the orphan partitions
      Iterator<Replica> it = orphaned.iterator();
      while (it.hasNext())
      {
        Replica replica = it.next();
        Node receiver = preferredAssignment.get(replica);
        // logger.info("replica:"+ replica + " preferred location:"+
        // receiver);
        if (receiver.capacity > receiver.currentlyAssigned
            && receiver.canAdd(replica))
        {
          receiver.currentlyAssigned = receiver.currentlyAssigned + 1;
          receiver.nonPreferred.add(replica);
          it.remove();
        }
      }
      // now iterate over nodes and remaining orphaned partitions and assign
      // partitions randomly
      // Better to iterate over orphaned partitions first
      it = orphaned.iterator();
      while (it.hasNext())
      {
        Replica replica = it.next();
        int startIndex = (replica.hashCode() & 0x7FFFFFFF) % liveNodesList.size();
        for (int index = startIndex; index < startIndex + liveNodesList.size(); index++)
        {
          Node receiver = liveNodesList.get(index % liveNodesList.size());
          if (receiver.capacity > receiver.currentlyAssigned
              && receiver.canAdd(replica))
          {
            receiver.currentlyAssigned = receiver.currentlyAssigned + 1;
            receiver.nonPreferred.add(replica);
            it.remove();
            break;
          }
        }
      }
      if (orphaned.size() > 0)
      {
        logger.warn("could not assign nodes to partitions: " + orphaned);
      }

      // now iterator over nodes and move extra load

      for (Node donor : liveNodesList)
      {
        if (donor.capacity < donor.currentlyAssigned)
        {
          Collections.sort(donor.nonPreferred);
          it = donor.nonPreferred.iterator();
          while (it.hasNext())
          {
            Replica replica = it.next();
            int startIndex = (replica.hashCode() & 0x7FFFFFFF)
                % liveNodesList.size();

            for (int index = startIndex; index < startIndex
                + liveNodesList.size(); index++)
            {
              Node receiver = liveNodesList.get(index % liveNodesList.size());
              if (receiver.canAdd(replica))
              {
                receiver.currentlyAssigned = receiver.currentlyAssigned + 1;
                receiver.nonPreferred.add(replica);
                it.remove();
                break;
              }
            }
            if (donor.capacity >= donor.currentlyAssigned)
            {
              break;
            }
          }
          if (donor.capacity < donor.currentlyAssigned)
          {
            logger.warn("Could not take partitions out of node:" + donor.id);
          }
        }
      }
      for (String partition : partitions)
      {
        znRecord.setMapField(partition, new TreeMap<String, String>());
        znRecord.setListField(partition, new ArrayList<String>());
      }
      for (Node node : liveNodesList)
      {
        for (Replica replica : node.preferred)
        {
          znRecord.getMapField(replica.partition).put(node.id, replica.state);
        }
        for (Replica replica : node.nonPreferred)
        {
          znRecord.getMapField(replica.partition).put(node.id, replica.state);
        }
      }

      for (String state : states.keySet())
      {
        int count = states.get(state);
        for (int replicaId = 0; replicaId < count; replicaId++)
        {
          for (Node node : liveNodesList)
          {
            for (Replica replica : node.preferred)
            {
              if (replica.state.equals(state) && replicaId == replica.replicaId)
              {
                znRecord.getListField(replica.partition).add(node.id);
              }
            }
            for (Replica replica : node.nonPreferred)
            {
              znRecord.getListField(replica.partition).add(node.id);
            }
          }
        }
      }
      return znRecord;

    }

    private Map<Replica, Node> computeExistingNonPreferredPlacement(
        LinkedHashMap<String, Integer> states,
        Map<String, Map<String, String>> currentMapping,
        Map<String, Node> nodeMap, Map<Replica, Node> preferredAssignment,
        Map<Replica, Node> existingPreferredAssignment)
    {

      Map<Replica, Node> existingNonPreferredAssignment = new TreeMap<Replica, Node>();
      for (String partition : currentMapping.keySet())
      {
        Map<String, String> nodeStateMap = currentMapping.get(partition);
        for (String nodeId : nodeStateMap.keySet())
        {
          Node node = nodeMap.get(nodeId);
          String state = nodeStateMap.get(nodeId);
          Integer count = states.get(state);
          // check if its in one of the preferred position
          for (int i = 0; i < count; i++)
          {
            Replica replica = new Replica(partition, state, i);
            if (!existingPreferredAssignment.containsKey(replica))
            {
              existingNonPreferredAssignment.put(replica, node);
              node.nonPreferred.add(replica);
              break;
            }
          }
        }
      }
      return existingNonPreferredAssignment;
    }

    private Set<Replica> computeOrphaned(
        final LinkedHashMap<String, Integer> states,
        final Map<String, Map<String, String>> currentMapping,
        Map<String, Node> nodeMap, Map<Replica, Node> preferredAssignment)
    {
      Set<Replica> orphanedPartitions = new TreeSet<Replica>(
          preferredAssignment.keySet());
      for (String partition : currentMapping.keySet())
      {
        Map<String, String> nodeStateMap = currentMapping.get(partition);
        for (String node : nodeStateMap.keySet())
        {
          String state = nodeStateMap.get(node);
          Integer count = states.get(state);
          // remove from orphaned if possible
          for (int i = 0; i < count; i++)
          {
            Replica replica = new Replica(partition, state, i);
            if (orphanedPartitions.contains(replica))
            {
              orphanedPartitions.remove(replica);
              break;
            }
          }
        }
      }
      return orphanedPartitions;
    }

    private Map<Replica, Node> computeExistingPreferredPlacement(
        final LinkedHashMap<String, Integer> states,
        final Map<String, Map<String, String>> currentMapping,
        Map<String, Node> nodeMap, Map<Replica, Node> preferredAssignment)
    {
      Map<Replica, Node> existingPreferredAssignment = new TreeMap<Replica, Node>();
      for (String partition : currentMapping.keySet())
      {
        Map<String, String> nodeStateMap = currentMapping.get(partition);
        for (String nodeId : nodeStateMap.keySet())
        {
          Node node = nodeMap.get(nodeId);
          node.currentlyAssigned = node.currentlyAssigned + 1;
          String state = nodeStateMap.get(nodeId);
          Integer count = states.get(state);
          // check if its in one of the preferred position
          for (int i = 0; i < count; i++)
          {
            Replica replica = new Replica(partition, state, i);
            if (preferredAssignment.containsKey(replica)
                && !existingPreferredAssignment.containsKey(replica))
            {
              existingPreferredAssignment.put(replica, node);
              node.preferred.add(replica);
              break;
            }
          }
        }
      }
      return existingPreferredAssignment;
    }

    private Map<Replica, Node> computePreferredPlacement(
        final List<String> partitions,
        final LinkedHashMap<String, Integer> states, final List<String> allNodes,
        Map<String, Node> nodeMap)
    {
      Map<Replica, Node> preferredMapping;
      preferredMapping = new HashMap<Replica, Node>();
      int partitionId = 0;

      for (String partition : partitions)
      {
        int replicaId = 0;
        for (String state : states.keySet())
        {
          for (int i = 0; i < states.get(state); i++)
          {
            Replica replica = new Replica(partition, state, i);
            int index = (partitionId + replicaId) % allNodes.size();
            preferredMapping.put(replica, nodeMap.get(allNodes.get(index)));
            replicaId = replicaId + 1;
          }
        }
        partitionId = partitionId + 1;
      }
      return preferredMapping;
    }

    /**
     * Counts the total number of replicas given a state-count mapping
     * 
     * @param states
     * @return
     */
    private int countStateReplicas(LinkedHashMap<String, Integer> states)
    {
      int total = 0;
      for (Integer count : states.values())
      {
        total += count;
      }
      return total;
    }

    class Node
    {

      public int currentlyAssigned;
      public int capacity;
      private String id;
      boolean isAlive;
      private List<Replica> preferred;
      private List<Replica> nonPreferred;

      public Node(String id)
      {
        preferred = new ArrayList<Replica>();
        nonPreferred = new ArrayList<Replica>();
        currentlyAssigned = 0;
        isAlive = false;
        this.id = id;
      }

      public boolean canAdd(Replica replica)
      {
        if (!isAlive)
        {
          return false;
        }
        if (currentlyAssigned >= capacity)
        {
          return false;
        }
        for (Replica r : preferred)
        {
          if (r.partition == replica.partition)
          {
            return false;
          }
        }
        for (Replica r : nonPreferred)
        {
          if (r.partition == replica.partition)
          {
            return false;
          }
        }
        return true;
      }

      @Override
      public String toString()
      {
        StringBuilder sb = new StringBuilder();
        sb.append("##########\nname=").append(id).append("\npreferred:")
            .append(preferred.size()).append("\nnonpreferred:")
            .append(nonPreferred.size());
        return sb.toString();
      }
    }

    class Replica implements Comparable<Replica>
    {

      private String partition;
      private String state;
      private int replicaId;
      private String format;

      public Replica(String partition, String state, int replicaId)
      {
        this.partition = partition;
        this.state = state;
        this.replicaId = replicaId;
        this.format = partition + "|" + state + "|" + replicaId;
      }

      @Override
      public String toString()
      {
        return format;
      }

      @Override
      public boolean equals(Object that)
      {
        if (that instanceof Replica)
        {
          return this.format.equals(((Replica) that).format);
        }
        return false;
      }

      @Override
      public int hashCode()
      {
        return this.format.hashCode();
      }

      @Override
      public int compareTo(Replica that)
      {
        if (that instanceof Replica)
        {
          return this.format.compareTo(((Replica) that).format);
        }
        return -1;
      }
    }
  }
  
  /**
   * 
   * @return state count map: state->count
   */
  LinkedHashMap<String, Integer> stateCount(StateModelDefinition stateModelDef, 
      int liveNodesNb,
      int totalReplicas) {
    LinkedHashMap<String, Integer> stateCountMap = new LinkedHashMap<String, Integer>();
    List<String> statesPriorityList = stateModelDef.getStatesPriorityList();
    
    int replicas = totalReplicas;
    for (String state : statesPriorityList) {
      String num = stateModelDef.getNumInstancesPerState(state);
      if ("N".equals(num)) {
        stateCountMap.put(state, liveNodesNb);
      } else if ("R".equals(num)) {
        // wait until we get the counts for all other states
        continue;
      } else {
        int stateCount = -1;
        try {
          stateCount = Integer.parseInt(num);
        } catch (Exception e) {
          // LOG.error("Invalid count for state: " + state + ", count: " + num + ", use -1 instead");
        }
        
        if (stateCount > 0) {
          stateCountMap.put(state, stateCount);
          replicas -= stateCount;
        }
      }
    }

    // get state count for R
    for (String state : statesPriorityList) {
      String num = stateModelDef.getNumInstancesPerState(state);
      if ("R".equals(num)) {
        stateCountMap.put(state, replicas);
        // should have at most one state using R
        break;
      }
    }
    return stateCountMap;
  }
  
  Map<String, Map<String, String>> currentMapping(CurrentStateOutput currentStateOutput, 
      String resourceName, List<String> partitions, Map<String, Integer> stateCountMap) {
    
    Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();
    
    for (String partition : partitions) {
      Map<String, String> curStateMap = currentStateOutput.getCurrentStateMap(resourceName, new Partition(partition));
      map.put(partition, new HashMap<String, String>());
      for (String node : curStateMap.keySet()) {
        String state = curStateMap.get(node);
        if (stateCountMap.containsKey(state)) {
          map.get(partition).put(node, state);
        }
      }
      
      Map<String, String> pendingStateMap = currentStateOutput.getPendingStateMap(resourceName, new Partition(partition));
      for (String node : pendingStateMap.keySet()) {
        String state = pendingStateMap.get(node);
        if (stateCountMap.containsKey(state)) {
          map.get(partition).put(node, state);
        }
      }
    }
    return map;
  }
  
}
