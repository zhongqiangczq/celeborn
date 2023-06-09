/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.plugin.flink;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.client.listener.WorkerStatusListener;
import org.apache.celeborn.client.listener.WorkersStatus;
import org.apache.celeborn.common.meta.ShufflePartitionLocationInfo;
import org.apache.celeborn.common.meta.WorkerInfo;

public class ShuffleResourceTracker implements WorkerStatusListener {
  private static final Logger LOG = LoggerFactory.getLogger(ShuffleResourceTracker.class);
  private final ExecutorService executorService;
  private final LifecycleManager lifecycleManager;
  // JobID -> ShuffleResourceListener
  private final Map<JobID, JobShuffleResourceListener> shuffleResourceListeners =
      new ConcurrentHashMap<>();
  private static final int MAX_RETRY_TIMES = 3;

  public ShuffleResourceTracker(
      ExecutorService executorService, LifecycleManager lifecycleManager) {
    this.executorService = executorService;
    this.lifecycleManager = lifecycleManager;
    lifecycleManager.registerWorkerStatusListener(this);
  }

  public void registerJob(
      JobID jobID,
      Function<Collection<ResultPartitionID>, CompletableFuture<?>>
          stopTrackingAndReleasePartitionsHandler) {
    shuffleResourceListeners.put(
        jobID,
        new JobShuffleResourceListener(
            jobID, executorService, stopTrackingAndReleasePartitionsHandler));
  }

  public void addPartitionResource(
      JobID jobId, int shuffleId, int partitionId, ResultPartitionID partitionID) {
    JobShuffleResourceListener shuffleResourceListener = shuffleResourceListeners.get(jobId);
    shuffleResourceListener.addPartitionResource(shuffleId, partitionId, partitionID);
  }

  public void removePartitionResource(JobID jobID, int shuffleId, int partitionId) {
    JobShuffleResourceListener shuffleResourceListener = shuffleResourceListeners.get(jobID);
    if (shuffleResourceListener != null) {
      shuffleResourceListener.removePartitionResource(shuffleId, partitionId);
    }
  }

  public JobShuffleResourceListener getJobResourceListener(JobID jobID) {
    return shuffleResourceListeners.get(jobID);
  }

  public void unRegisterJob(JobID jobID) {
    shuffleResourceListeners.remove(jobID);
  }

  @Override
  public void notifyChangedWorkersStatus(WorkersStatus workersStatus) {
    try {
      List<WorkerInfo> unknownWorkers = workersStatus.unknownWorkers;
      LOG.debug("resource tracker:{}", unknownWorkers);
      if (unknownWorkers != null && !unknownWorkers.isEmpty()) {
        // untrack by job
        for (Map.Entry<JobID, JobShuffleResourceListener> entry :
            shuffleResourceListeners.entrySet()) {
          Set<ResultPartitionID> partitionIds = new HashSet<>();
          JobShuffleResourceListener shuffleResourceListener = entry.getValue();
          for (Map.Entry<Integer, Map<Integer, ResultPartitionID>> mapEntry :
              shuffleResourceListener.getResultPartitionMap().entrySet()) {
            int shuffleId = mapEntry.getKey();
            if (!mapEntry.getValue().isEmpty()) {
              for (WorkerInfo unknownWorker : unknownWorkers) {
                Map<WorkerInfo, ShufflePartitionLocationInfo> shuffleAllocateInfo =
                    lifecycleManager.workerSnapshots(shuffleId);
                // shuffleResourceListener may release when the shuffle is ended
                if (shuffleAllocateInfo != null) {
                  ShufflePartitionLocationInfo shufflePartitionLocationInfo =
                      shuffleAllocateInfo.get(unknownWorker);
                  if (shufflePartitionLocationInfo != null) {
                    // TODO if we support partition replica for map partition we need refactor this
                    //  Currently we only untrack master partitions for map partition
                    shufflePartitionLocationInfo
                        .removeAndGetAllMasterPartitionIds()
                        .forEach(
                            id -> {
                              ResultPartitionID resultPartitionId =
                                  shuffleResourceListener.removePartitionResource(shuffleId, id);
                              if (resultPartitionId != null) {
                                partitionIds.add(resultPartitionId);
                              }
                            });
                  }
                }
              }
            }
          }

          shuffleResourceListener.notifyStopTrackingPartitions(partitionIds, MAX_RETRY_TIMES);
        }
      }
    } catch (Throwable e) {
      // listener never throw exception
      LOG.error("Failed to handle unknown workers, message: {}.", e.getMessage(), e);
    }
  }

  public static class JobShuffleResourceListener {

    private final JobID jobID;
    private final ExecutorService executorService;
    // celeborn shuffleId -> partitionId -> Flink ResultPartitionID
    private Map<Integer, Map<Integer, ResultPartitionID>> resultPartitionMap =
        new ConcurrentHashMap<>();
    private Function<Collection<ResultPartitionID>, CompletableFuture<?>>
        stopTrackingAndReleasePartitionsHandler;

    public JobShuffleResourceListener(
        JobID jobID,
        ExecutorService executorService,
        Function<Collection<ResultPartitionID>, CompletableFuture<?>>
            stopTrackingAndReleasePartitionsHandler) {
      this.jobID = jobID;
      this.executorService = executorService;
      this.stopTrackingAndReleasePartitionsHandler = stopTrackingAndReleasePartitionsHandler;
    }

    public void addPartitionResource(
        int shuffleId, int partitionId, ResultPartitionID partitionID) {
      Map<Integer, ResultPartitionID> shufflePartitionMap =
          resultPartitionMap.computeIfAbsent(shuffleId, (s) -> new ConcurrentHashMap<>());
      shufflePartitionMap.put(partitionId, partitionID);
    }

    private void notifyStopTrackingPartitions(
        Set<ResultPartitionID> partitionIDS, int remainingRetries) {
      LOG.debug("notifyStopTrackingPartitions:{}, {}", partitionIDS, remainingRetries);
      if (partitionIDS == null || partitionIDS.isEmpty()) {
        return;
      }

      LOG.info(
          "jobId: {}, stop tracking partitions {}.",
          jobID,
          Arrays.toString(partitionIDS.toArray()));

      int count = remainingRetries - 1;
      try {
        CompletableFuture<?> future = stopTrackingAndReleasePartitionsHandler.apply(partitionIDS);
        future.whenCompleteAsync(
            (ignored, throwable) -> {
              if (throwable == null) {
                return;
              }

              if (count == 0) {
                LOG.error(
                    "jobId: {}, Failed to stop tracking partitions {}.",
                    jobID,
                    Arrays.toString(partitionIDS.toArray()));
                return;
              }
              notifyStopTrackingPartitions(partitionIDS, count);
            },
            executorService);
      } catch (Throwable throwable) {
        if (count == 0) {
          LOG.error(
              "jobId: {}, Failed to stop tracking partitions {}.",
              jobID,
              Arrays.toString(partitionIDS.toArray()),
              throwable);
          return;
        }
        notifyStopTrackingPartitions(partitionIDS, count);
      }
    }

    public Map<Integer, Map<Integer, ResultPartitionID>> getResultPartitionMap() {
      return resultPartitionMap;
    }

    public ResultPartitionID removePartitionResource(int shuffleId, int partitionId) {
      Map<Integer, ResultPartitionID> partitionIDMap = resultPartitionMap.get(shuffleId);
      if (partitionIDMap != null) {
        return partitionIDMap.remove(partitionId);
      }

      return null;
    }
  }
}
