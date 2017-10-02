<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  
   http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Integrating the S3A Committers with Spark

This document looks at the whole issue of "how to integrate the Hadoop S3A Committers" 
with Apache Spark —it is intended to apply to any custom `PathOutputCommitter`
implementation.


## Background: Hadoop

Hadoop has two MapReduce APIs, MRv1 and MRv2 (not to be distnguished from the v1/v2 commit
algorithms.)

The "original "
