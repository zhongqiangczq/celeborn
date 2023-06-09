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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.io.network.partition.PartitionTrackerFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.TaskInputsOutputsDescriptor;
import org.apache.flink.util.AbstractID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.plugin.flink.config.PluginConf;
import org.apache.celeborn.plugin.flink.utils.FlinkUtils;
import org.apache.celeborn.plugin.flink.utils.ThreadUtils;
import org.apache.celeborn.plugin.flink.utils.Utils;

/** The shuffle master implementation for remote shuffle service plugin. */
public class RemoteShuffleMaster implements ShuffleMaster<RemoteShuffleDescriptor> {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteShuffleMaster.class);

  private final Configuration flinkConfiguration;
  // Flink JobId -> Celeborn register shuffleIds
  private Map<JobID, Set<Integer>> jobShuffleIds = new ConcurrentHashMap<>();
  private static String celebornAppId;
  private static volatile LifecycleManager lifecycleManager;
  private static volatile ShuffleResourceTracker shuffleResourceTracker;
  private static ScheduledThreadPoolExecutor executor =
      new ScheduledThreadPoolExecutor(
          1,
          ThreadUtils.createFactoryWithDefaultExceptionHandler(
              "remote-shuffle-master-executor", LOG));
  private JobID jobID;
  private static ShuffleTaskInfo shuffleTaskInfo = new ShuffleTaskInfo();
  private FatalErrorHandler fatalErrorHandler;
  private RemoteShuffleJobMasterPartitionTracker partitionTrackerFactory;
  private final long rssMetaServiceTimestamp;

  public RemoteShuffleMaster(Configuration flinkConfiguration) {
    this.flinkConfiguration = flinkConfiguration;
    this.rssMetaServiceTimestamp = System.currentTimeMillis();
  }

  @Override
  public void initialize(
      AbstractID flinkJobId, Executor mainThreadExecutor, FatalErrorHandler fatalErrorHandler)
      throws Exception {
    jobID = new JobID(flinkJobId.getBytes());
    partitionTrackerFactory = new RemoteShuffleJobMasterPartitionTracker(this);
    this.fatalErrorHandler = fatalErrorHandler;
    if (lifecycleManager == null) {
      synchronized (RemoteShuffleMaster.class) {
        if (lifecycleManager == null) {
          // use first jobID as celeborn shared appId for all other flink jobs
          celebornAppId = FlinkUtils.toCelebornAppId(rssMetaServiceTimestamp, jobID);
          LOG.info("CelebornAppId: {}", celebornAppId);
          CelebornConf celebornConf = FlinkUtils.toCelebornConf(flinkConfiguration);
          lifecycleManager = new LifecycleManager(celebornAppId, celebornConf);
          if (celebornConf.pushReplicateEnabled()) {
            fatalErrorHandler.onFatalError(
                new RuntimeException("Currently replicate shuffle data is unsupported for flink."));
            return;
          }
          this.shuffleResourceTracker = new ShuffleResourceTracker(executor, lifecycleManager);
        }
      }
    }

    Set<Integer> previousShuffleIds = jobShuffleIds.putIfAbsent(jobID, new HashSet<>());
    if (previousShuffleIds != null) {
      throw new RuntimeException("Duplicated registration job: " + jobID);
    }
    shuffleResourceTracker.registerJob(
        jobID, partitionTrackerFactory::stopTrackingAndReleasePartitionsWithFuture);
  }

  @Override
  public void synchronizeWorkerStatus() throws Exception {
    //    Set<InstanceID> initialWorkers = jobMasterPartitionTracker.getRelatedShuffleWorkers();
    //    shuffleManagerClient.synchronizeWorkerStatus(initialWorkers);
  }

  public PartitionTrackerFactory createJobMasterPartitionTrackerFactory(
      org.apache.flink.api.common.JobID jobID) {
    return lookup -> {
      Utils.checkState(partitionTrackerFactory != null, "JobMasterPartitionTrackerFactory is null");
      return partitionTrackerFactory;
    };
  }

  @Override
  public CompletableFuture<RemoteShuffleDescriptor> registerPartitionWithProducer(
      PartitionDescriptor partitionDescriptor, ProducerDescriptor producerDescriptor) {

    CompletableFuture<RemoteShuffleDescriptor> completableFuture =
        CompletableFuture.supplyAsync(
            () -> {
              Set<Integer> shuffleIds = jobShuffleIds.get(jobID);
              if (shuffleIds == null) {
                throw new RuntimeException("Can not find job in lifecycleManager, job: " + jobID);
              }

              FlinkResultPartitionInfo resultPartitionInfo =
                  new FlinkResultPartitionInfo(jobID, partitionDescriptor, producerDescriptor);
              ShuffleResourceDescriptor shuffleResourceDescriptor =
                  shuffleTaskInfo.genShuffleResourceDescriptor(
                      resultPartitionInfo.getShuffleId(),
                      resultPartitionInfo.getTaskId(),
                      resultPartitionInfo.getAttemptId());

              synchronized (shuffleIds) {
                shuffleIds.add(shuffleResourceDescriptor.getShuffleId());
              }

              RemoteShuffleResource remoteShuffleResource =
                  new RemoteShuffleResource(
                      lifecycleManager.getRssMetaServiceHost(),
                      lifecycleManager.getRssMetaServicePort(),
                      rssMetaServiceTimestamp,
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

  public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {
    executor.execute(
        () -> {
          if (!(shuffleDescriptor instanceof RemoteShuffleDescriptor)) {
            LOG.error(
                "Only RemoteShuffleDescriptor is supported {}.",
                shuffleDescriptor.getClass().getName());
            this.fatalErrorHandler.onFatalError(
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
  public MemorySize getShuffleMemoryForTask(
      TaskInputsOutputsDescriptor taskInputsOutputsDescriptor) {
    for (ResultPartitionType partitionType :
        taskInputsOutputsDescriptor.getOutputResultPartitionTypes().values()) {
      if (!partitionType.isBlocking()) {
        fatalErrorHandler.onFatalError(
            new RuntimeException(
                "Blocking result partition type expected but found " + partitionType));
      }
    }

    int numResultPartitions = taskInputsOutputsDescriptor.getNumbersOfResultSubpartitions().size();
    long numBytesPerPartition =
        FlinkUtils.byteStringValueAsBytes(
            flinkConfiguration, PluginConf.MEMORY_PER_RESULT_PARTITION);
    long numBytesForOutput = numBytesPerPartition * numResultPartitions;

    int numInputGates = taskInputsOutputsDescriptor.getNumbersOfInputGateChannels().size();
    long numBytesPerGate =
        FlinkUtils.byteStringValueAsBytes(flinkConfiguration, PluginConf.MEMORY_PER_INPUT_GATE);
    long numBytesForInput = numBytesPerGate * numInputGates;

    LOG.debug(
        "Announcing number of bytes {} for output and {} for input.",
        numBytesForOutput,
        numBytesForInput);

    return new MemorySize(numBytesForInput + numBytesForOutput);
  }

  @Override
  public void close() {
    LOG.info("Unregister job: {}.", jobID);
    Set<Integer> shuffleIds = jobShuffleIds.remove(jobID);
    if (shuffleIds != null) {
      executor.execute(
          () -> {
            try {
              synchronized (shuffleIds) {
                for (Integer shuffleId : shuffleIds) {
                  lifecycleManager.handleUnregisterShuffle(celebornAppId, shuffleId);
                  shuffleTaskInfo.removeExpiredShuffle(shuffleId);
                  shuffleResourceTracker.unRegisterJob(jobID);
                }
              }
            } catch (Throwable throwable) {
              LOG.error("Encounter an error when unregistering job: {}.", jobID, throwable);
            }
          });
    }
  }

  public JobID getJobId() {
    return jobID;
  }
}
