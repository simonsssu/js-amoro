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

package org.apache.amoro.server.manager.kyuubi;

import org.apache.kyuubi.client.KyuubiRestClient;
import org.apache.kyuubi.client.auth.AuthHeaderGenerator;

public interface KyuubiRestHelper {
  String HS2_PROXY_USER = "hive.server2.proxy.user";

  String endpoint();

  String serverPrincipalHost();

  int REST_SOCKET_TIMEOUT = 120000;

  int REST_CONNECT_TIMEOUT = 30000;

  /** If you want to use custom auth header generator, please implement this method. */
  default AuthHeaderGenerator authHeaderGenerator() {
    return null;
  }

  default KyuubiRestClient newSpnegoBatchRestClient() {
    return KyuubiRestClient.builder(endpoint())
        .connectionTimeout(REST_CONNECT_TIMEOUT)
        .socketTimeout(REST_SOCKET_TIMEOUT)
        .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.SPNEGO)
        .spnegoHost(serverPrincipalHost())
        .build();
  }

  default KyuubiRestClient newBasicBatchRestClient(String username, String passwd) {
    return KyuubiRestClient.builder(endpoint())
        .connectionTimeout(REST_CONNECT_TIMEOUT)
        .socketTimeout(REST_SOCKET_TIMEOUT)
        .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.BASIC)
        .username(username)
        .password(passwd)
        .build();
  }

  default KyuubiRestClient newCustomBatchRestClient() {
    return KyuubiRestClient.builder(endpoint())
        .connectionTimeout(REST_CONNECT_TIMEOUT)
        .socketTimeout(REST_SOCKET_TIMEOUT)
        .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.CUSTOM)
        .authHeaderGenerator(authHeaderGenerator())
        .build();
  }
}
