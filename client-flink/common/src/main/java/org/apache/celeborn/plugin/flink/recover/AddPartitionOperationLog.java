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

package org.apache.celeborn.plugin.flink.recover;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import org.apache.celeborn.client.recover.OperationLog;

public class AddPartitionOperationLog implements OperationLog {

  private static final long serialVersionUID = -1474156548190360574L;
  private JobID jobId;
  private int shuffleId;
  private int partitionId;
  private ResultPartitionID partitionID;

  public AddPartitionOperationLog(
      JobID jobId, int shuffleId, int partitionId, ResultPartitionID partitionID) {
    this.jobId = jobId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.partitionID = partitionID;
  }

  public JobID getJobId() {
    return jobId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public ResultPartitionID getPartitionID() {
    return partitionID;
  }

  @Override
  public Type getType() {
    return Type.ADD_PARTITION;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("AddPartitionOperationLog{");
    sb.append("jobId=").append(jobId);
    sb.append(", shuffleId=").append(shuffleId);
    sb.append(", partitionId=").append(partitionId);
    sb.append(", partitionID=").append(partitionID);
    sb.append('}');
    return sb.toString();
  }
}
