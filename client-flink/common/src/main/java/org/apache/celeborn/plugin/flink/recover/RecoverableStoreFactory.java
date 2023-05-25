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

import java.io.IOException;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;

import org.apache.celeborn.client.recover.DummyRecoverableStore;
import org.apache.celeborn.client.recover.RecoverableStore;
import org.apache.celeborn.plugin.flink.RemoteShuffleMaster;

public class RecoverableStoreFactory {
  public static RecoverableStore createOperationLogStore(
      String jobId, Configuration config, RemoteShuffleMaster remoteShuffleMaster)
      throws IOException {
    if (config.getBoolean(JobManagerOptions.JM_FAILOVER_ENABLED)) {
      return new FileSystemRecoverableStore(jobId, config, remoteShuffleMaster);
    } else {
      return new DummyRecoverableStore();
    }
  }
}
