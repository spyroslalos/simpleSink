##
## Puppet managed
##
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# If this file is placed at FLUME_CONF_DIR/flume-env.sh, it will be sourced
# during Flume startup.

# Enviroment variables can be set here.

#JAVA_HOME=/usr/lib/jvm/java-6-sun
#JAVA_HOME=/usr/lib/jvm/jre-1.6.0
#JAVA_HOME="/usr/bin/java"
JAVA_HOME = /usr/lib/jvm/jdk1.7.0_04/

# Give Flume more memory and pre-allocate, enable remote monitoring via JMX
JAVA_OPTS="-Xms20m -Xmx50m -Duser.timezone=Europe/Zurich -Dsun.net.spi.nameservice.provider.1=dns,sun -Dsun.net.inetaddr.ttl=0 -Dsun.net.inetaddr.negative.ttl=0 -Dnetworkaddress.cache.ttl=0 -Dnetworkaddress.cache.negative.ttl=0"

# Note that the Flume conf directory is always included in the classpath.
FLUME_CLASSPATH=/usr/share/java/*:/usr/share/elasticsearch/lib/*:/etc/flume-ng/hbsink/conf/skeleton-flume-extra/target/*:/etc/flume-ng/hbsink/conf/skeleton-flume-extra/lib/*

