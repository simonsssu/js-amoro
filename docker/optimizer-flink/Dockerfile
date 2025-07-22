#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.

ARG FLINK_VERSION=1.20.0-java8

FROM ecr.vip.ebayc3.com/rheos/flink:1.18.1.1 

ARG MAVEN_MIRROR=https://ebaycentral.qa.ebay.com/content/repositories/central
ARG OPTIMIZER_JOB=optimizer-job.jar

ENV KRB5CCNAME /opt/settings/kite2/krb5cc_cache
ENV KRB5PRINCIPAL b_rheos@PROD.EBAY.COM

COPY ./docker/flink-shaded-hadoop-2-uber-2.7.5-10.0.jar $FLINK_HOME/lib
COPY $OPTIMIZER_JOB $FLINK_HOME/usrlib/optimizer-job.jar

