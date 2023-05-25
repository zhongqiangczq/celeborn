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

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.shuffle.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.client.recover.OperationLog;
import org.apache.celeborn.client.recover.RecoverableStore;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.plugin.flink.recover.AppRegisterOperationLog;
import org.apache.celeborn.plugin.flink.recover.RecoverableStoreFactory;
import org.apache.celeborn.plugin.flink.recover.UnregisterJobOperationLog;
import org.apache.celeborn.plugin.flink.utils.FlinkUtils;
import org.apache.celeborn.plugin.flink.utils.ThreadUtils;

public class RemoteShuffleMaster implements ShuffleMaster<RemoteShuffleDescriptor> {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteShuffleMaster.class);
  private final ShuffleMasterContext shuffleMasterContext;
  private final boolean jobManagerFailoverEnabled;
  private final boolean isApplicationMode;
  private String celebornAppId;
  private volatile LifecycleManager lifecycleManager;
  private ShuffleTaskInfo shuffleTaskInfo;
  private ShuffleResourceTracker shuffleResourceTracker;
  private final ScheduledThreadPoolExecutor executor =
      new ScheduledThreadPoolExecutor(
          1,
          ThreadUtils.createFactoryWithDefaultExceptionHandler(
              "remote-shuffle-master-executor", LOG));
  private final ResultPartitionAdapter resultPartitionDelegation;
  private final long lifecycleManagerTimestamp;
  private CelebornConf celebornConf;
  private RecoverableStore recoverableStore;
  private Map<JobID, Long> expiredJobIds = new ConcurrentHashMap<>();

  public RemoteShuffleMaster(
      ShuffleMasterContext shuffleMasterContext, ResultPartitionAdapter resultPartitionDelegation) {
    this.shuffleMasterContext = shuffleMasterContext;
    this.resultPartitionDelegation = resultPartitionDelegation;
    this.lifecycleManagerTimestamp = System.currentTimeMillis();
    this.celebornConf = FlinkUtils.toCelebornConf(shuffleMasterContext.getConfiguration());
    // if not set, set to true as default for flink
    celebornConf.setIfMissing(CelebornConf.CLIENT_CHECKED_USE_ALLOCATED_WORKERS(), true);
    if (celebornConf.clientPushReplicateEnabled()) {
      shuffleMasterContext.onFatalError(
          new RuntimeException("Currently replicate shuffle data is unsupported for flink."));
    }

    this.isApplicationMode = FlinkUtils.isApplicationMode(shuffleMasterContext.getConfiguration());
    if (isApplicationMode) {
      this.jobManagerFailoverEnabled =
          FlinkUtils.jobManagerFailoverEnabled(shuffleMasterContext.getConfiguration());
    } else {
      this.jobManagerFailoverEnabled =
          FlinkUtils.jobManagerFailoverEnabled(shuffleMasterContext.getConfiguration())
              && celebornConf.clientFlinkSupportSessionModeFailover();
    }

    LOG.debug("shuffleMasterContext: {}", shuffleMasterContext.getConfiguration());
  }

  @Override
  public void registerJob(JobShuffleContext context) {
    JobID jobID = context.getJobId();
    try {
      if (lifecycleManager == null) {
        synchronized (RemoteShuffleMaster.class) {
          if (lifecycleManager == null) {
            recover(jobID);
          }
        }
      }
    } catch (Exception e) {
      shuffleMasterContext.onFatalError(
          new RuntimeException("Can not recover from operation log.", e));
    }

    if (expiredJobIds.get(jobID) != null) {
      expiredJobIds.remove(jobID);
    }

    LOG.info("Register job: {}.", jobID);
    shuffleResourceTracker.registerJob(context);
  }

  @Override
  public void unregisterJob(JobID jobID) {
    LOG.info("Unregister job: {}.", jobID);
    Set<Integer> shuffleIds = shuffleResourceTracker.getJobShuffleIds(jobID);
    if (shuffleIds != null) {
      executor.execute(
          () -> {
            try {
              expireJob(jobID, !jobManagerFailoverEnabled);
            } catch (Throwable throwable) {
              LOG.error("Encounter an error when unregistering job: {}.", jobID, throwable);
            }
          });
    }
  }

  @Override
  public CompletableFuture<RemoteShuffleDescriptor> registerPartitionWithProducer(
      JobID jobID, PartitionDescriptor partitionDescriptor, ProducerDescriptor producerDescriptor) {
    CompletableFuture<RemoteShuffleDescriptor> completableFuture =
        CompletableFuture.supplyAsync(
            () -> {
              FlinkResultPartitionInfo resultPartitionInfo =
                  new FlinkResultPartitionInfo(jobID, partitionDescriptor, producerDescriptor);
              ShuffleResourceDescriptor shuffleResourceDescriptor =
                  shuffleTaskInfo.genShuffleResourceDescriptor(
                      resultPartitionInfo.getShuffleId(),
                      resultPartitionInfo.getTaskId(),
                      resultPartitionInfo.getAttemptId());

              RemoteShuffleResource remoteShuffleResource =
                  new RemoteShuffleResource(
                      lifecycleManager.getHost(),
                      lifecycleManager.getPort(),
                      lifecycleManagerTimestamp,
                      shuffleResourceDescriptor);

              shuffleResourceTracker.addPartitionResource(
                  jobID,
                  shuffleResourceDescriptor.getShuffleId(),
                  shuffleResourceDescriptor.getPartitionId(),
                  resultPartitionInfo.getResultPartitionId());

              return new RemoteShuffleDescriptor(
                  celebornAppId,
                  jobID,
                  resultPartitionInfo.getShuffleId(),
                  resultPartitionInfo.getResultPartitionId(),
                  remoteShuffleResource);
            },
            executor);

    return completableFuture;
  }

  @Override
  public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {
    executor.execute(
        () -> {
          if (!(shuffleDescriptor instanceof RemoteShuffleDescriptor)) {
            LOG.error(
                "Only RemoteShuffleDescriptor is supported {}.",
                shuffleDescriptor.getClass().getName());
            shuffleMasterContext.onFatalError(
                new RuntimeException("Illegal shuffle descriptor type."));
            return;
          }
          try {
            RemoteShuffleDescriptor descriptor = (RemoteShuffleDescriptor) shuffleDescriptor;
            RemoteShuffleResource shuffleResource = descriptor.getShuffleResource();
            ShuffleResourceDescriptor resourceDescriptor =
                shuffleResource.getMapPartitionShuffleDescriptor();
            LOG.debug("release partition resource: {}.", resourceDescriptor);
            lifecycleManager.releasePartition(
                resourceDescriptor.getShuffleId(), resourceDescriptor.getPartitionId());
            shuffleResourceTracker.removePartitionResource(
                descriptor.getJobId(),
                resourceDescriptor.getShuffleId(),
                resourceDescriptor.getPartitionId());
          } catch (Throwable throwable) {
            // it is not a problem if we failed to release the target data partition
            // because the session timeout mechanism will do the work for us latter
            LOG.debug("Failed to release data partition {}.", shuffleDescriptor, throwable);
          }
        });
  }

  @Override
  public MemorySize computeShuffleMemorySizeForTask(
      TaskInputsOutputsDescriptor taskInputsOutputsDescriptor) {
    for (ResultPartitionType partitionType :
        taskInputsOutputsDescriptor.getPartitionTypes().values()) {
      boolean isBlockingShuffle =
          resultPartitionDelegation.isBlockingResultPartition(partitionType);
      if (!isBlockingShuffle) {
        throw new RuntimeException(
            "Blocking result partition type expected but found " + partitionType);
      }
    }

    int numResultPartitions = taskInputsOutputsDescriptor.getSubpartitionNums().size();
    CelebornConf conf = FlinkUtils.toCelebornConf(shuffleMasterContext.getConfiguration());
    long numBytesPerPartition = conf.clientFlinkMemoryPerResultPartition();
    long numBytesForOutput = numBytesPerPartition * numResultPartitions;

    int numInputGates = taskInputsOutputsDescriptor.getInputChannelNums().size();
    long numBytesPerGate = conf.clientFlinkMemoryPerInputGate();
    long numBytesForInput = numBytesPerGate * numInputGates;

    LOG.debug(
        "Announcing number of bytes {} for output and {} for input.",
        numBytesForOutput,
        numBytesForInput);

    return new MemorySize(numBytesForInput + numBytesForOutput);
  }

  @Override
  public void close() throws Exception {
    try {
      lifecycleManager.stop();
      recoverableStore.stop();
    } catch (Exception e) {
      LOG.warn("Encounter exception when shutdown: " + e.getMessage(), e);
    }

    ThreadUtils.shutdownExecutors(10, executor);
  }

  public boolean hasJobs() {
    return !expiredJobIds.isEmpty() && !shuffleResourceTracker.getJobs().isEmpty();
  }

  private void createLifecycleManager() {
    LOG.info(
        "CelebornAppId: {}, mode: {}, jobManagerFailover: {}",
        celebornAppId,
        shuffleMasterContext.getConfiguration().get(DeploymentOptions.TARGET),
        jobManagerFailoverEnabled);
    lifecycleManager = new LifecycleManager(celebornAppId, celebornConf, recoverableStore);
    shuffleResourceTracker =
        new ShuffleResourceTracker(executor, lifecycleManager, recoverableStore);
    lifecycleManager.registerWorkerStatusListener(shuffleResourceTracker);
  }

  void recover(JobID currentJobID) throws IOException {
    String pathId = null;
    if (isApplicationMode) {
      // use fixed jobID path persistent operation log
      pathId = currentJobID.toString();
    }
    recoverableStore =
        RecoverableStoreFactory.createOperationLogStore(
            pathId, shuffleMasterContext.getConfiguration(), this);
    shuffleTaskInfo = new ShuffleTaskInfo(recoverableStore);
    boolean needRecover = true;
    OperationLog operationLog = recoverableStore.readOperation();
    if (operationLog != null) {
      celebornAppId = ((AppRegisterOperationLog) operationLog).getCelebornAppId();
      recoverableStore.registerIdOperation(operationLog);
      recoverableStore.setRecoverFinished(false);
    } else {
      needRecover = false;
      celebornAppId = FlinkUtils.toCelebornAppId(lifecycleManagerTimestamp, currentJobID);
      recoverableStore.registerIdOperation(new AppRegisterOperationLog(celebornAppId));
      recoverableStore.setRecoverFinished(true);
    }

    createLifecycleManager();
    if (!jobManagerFailoverEnabled || !needRecover) {
      lifecycleManager.initialize();
      return;
    }

    while ((operationLog = recoverableStore.readOperation()) != null) {
      LOG.debug("Recover operationLog: {}", operationLog);
      if (operationLog instanceof UnregisterJobOperationLog) {
        UnregisterJobOperationLog unregisterJobOperationLog =
            (UnregisterJobOperationLog) operationLog;
        if (!unregisterJobOperationLog.isAlreadyReleased()) {
          expiredJobIds.put(unregisterJobOperationLog.getJobID(), 0L);
        } else {
          expireJob(unregisterJobOperationLog.getJobID(), true);
        }
      } else {
        try {
          shuffleResourceTracker.replay(operationLog);
          shuffleTaskInfo.replay(operationLog);
          lifecycleManager.replay(operationLog);
        } catch (Exception e) {
          LOG.warn(
              "Recover operationLog error, may due to in complete operation log, just ignore this.",
              e);
        }
      }
    }

    if (needRecover) {
      recoverableStore.setRecoverFinished(true);
      lifecycleManager.initialize();
    }

    try {
      if (!isApplicationMode) {
        // refresh unregister jobIds expire time
        for (JobID jobID : expiredJobIds.keySet()) {
          expiredJobIds.put(jobID, System.currentTimeMillis());
        }
        // expire with fix rate
        executor.scheduleAtFixedRate(
            () -> {
              long current = System.currentTimeMillis();
              boolean hasExpiredJob = false;
              for (Map.Entry<JobID, Long> entry : expiredJobIds.entrySet()) {
                if (current - entry.getValue() > 300 * 1000) {
                  expireJob(entry.getKey(), true);
                  hasExpiredJob = true;
                }
              }

              if (hasExpiredJob) {
                recoverableStore.clear();
              }
            },
            180,
            180,
            TimeUnit.SECONDS);
      }
    } catch (Exception e) {
      shuffleMasterContext.onFatalError(
          new RuntimeException("Can not recover from operation log.", e));
    }
  }

  private void expireJob(JobID jobID, boolean expireImmediately) {
    LOG.info("expire flink job: {}, expireImmediately: {}", jobID, expireImmediately);
    if (expireImmediately) {
      Set<Integer> shuffleIds = shuffleResourceTracker.getJobShuffleIds(jobID);
      for (Integer shuffleId : shuffleIds) {
        lifecycleManager.handleUnregisterShuffle(shuffleId);
        shuffleTaskInfo.removeExpiredShuffle(shuffleId);
      }
      shuffleResourceTracker.unRegisterJob(jobID);
      expiredJobIds.remove(jobID);
      recoverableStore.writeOperation(new UnregisterJobOperationLog(jobID, true));
    } else {
      expiredJobIds.put(jobID, System.currentTimeMillis());
      recoverableStore.writeOperation(new UnregisterJobOperationLog(jobID, false));
    }
  }
}
