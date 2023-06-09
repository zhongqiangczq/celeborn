package org.apache.celeborn.plugin.flink;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.io.network.partition.AbstractPartitionTracker;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.PartitionTrackerEntry;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.plugin.flink.utils.Utils;

public class RemoteShuffleJobMasterPartitionTracker
    extends AbstractPartitionTracker<ResourceID, ResultPartitionDeploymentDescriptor>
    implements JobMasterPartitionTracker {

  private static final Logger LOG =
      LoggerFactory.getLogger(RemoteShuffleJobMasterPartitionTracker.class);

  private final RemoteShuffleMaster shuffleMaster;

  public RemoteShuffleJobMasterPartitionTracker(RemoteShuffleMaster shuffleMaster) {
    this.shuffleMaster = shuffleMaster;
    LOG.info(
        "Using remote shuffle job master partition tracker for job {}", shuffleMaster.getJobId());
  }

  @Override
  public void startTrackingPartition(
      ResourceID resourceID,
      ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor) {
    Utils.checkNotNull(resultPartitionDeploymentDescriptor);
    LOG.debug("start tracking partitions:{}", resultPartitionDeploymentDescriptor);
    // blocking and PIPELINED_APPROXIMATE partitions require explicit partition release calls
    // reconnectable will be removed after FLINK-19895, see also {@link
    // ResultPartitionType#isReconnectable}.
    if (!resultPartitionDeploymentDescriptor.getPartitionType().isReconnectable()) {
      return;
    }

    Utils.checkState(
        resultPartitionDeploymentDescriptor.getShuffleDescriptor()
            instanceof RemoteShuffleDescriptor,
        "ShuffleDescriptor is not instanceof RemoteShuffleDescriptor");
    RemoteShuffleDescriptor shuffleDescriptor =
        (RemoteShuffleDescriptor) resultPartitionDeploymentDescriptor.getShuffleDescriptor();

    startTrackingPartition(
        resourceID, shuffleDescriptor.getResultPartitionID(), resultPartitionDeploymentDescriptor);
  }

  @Override
  public void stopTrackingAndReleasePartitions(Collection<ResultPartitionID> resultPartitionIds) {
    Utils.checkNotNull(resultPartitionIds);
    LOG.debug("stopTrackingAndReleasePartitions:{}", resultPartitionIds);
    // stop tracking partitions to be released.
    List<ShuffleDescriptor> shuffleDescriptors =
        stopTrackingPartitions(resultPartitionIds).stream()
            .map(entry -> entry.getMetaInfo().getShuffleDescriptor())
            .collect(Collectors.toList());

    shuffleMaster.releasePartitionExternally(shuffleDescriptors);
  }

  public CompletableFuture<?> stopTrackingAndReleasePartitionsWithFuture(
      Collection<ResultPartitionID> resultPartitionIds) {
    stopTrackingAndReleasePartitions(resultPartitionIds);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public Collection<PartitionTrackerEntry<ResourceID, ResultPartitionDeploymentDescriptor>>
      stopTrackingPartitionsFor(ResourceID taskExecutorId) {
    Utils.checkState(!isTrackingPartitionsFor(taskExecutorId), "");
    return Collections.emptyList();
  }

  @Override
  public void stopTrackingAndReleasePartitionsFor(ResourceID taskExecutorId) {
    Utils.checkState(!isTrackingPartitionsFor(taskExecutorId), "");
  }

  /** ShuffleMaster should not call this method. */
  @Override
  public void stopTrackingAndReleaseOrPromotePartitionsFor(ResourceID taskExecutorId) {
    Utils.checkState(!isTrackingPartitionsFor(taskExecutorId), "");
  }

  /**
   * TODO: This method should be override. We will not add @Override now to satisfy the compiler.
   */
  @Override
  public void stopTrackingAndReleaseAllPartitions() {
    // Notes that here we do not include the partitions on the unrelated workers.
    // These partition have no records in the manager side, thus even if we consider
    // them, the manager would do nothing.
    // If the worker recover later, these partitions would be handled via last timeout.
    LOG.info("stop tracking and release all partitions");
    Set<ResultPartitionID> resultPartitionIDs = new HashSet<>();
    listPartitions().forEach(entry -> resultPartitionIDs.add(entry.f0));

    LOG.info(
        "stop tracking and release all partitions, {} partitions in total",
        resultPartitionIDs.size());
    stopTrackingAndReleasePartitions(resultPartitionIDs);
  }
}
