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

# Building

Either build the local hadoop/spark versions you want, or choose a profile which refers to public releases

note: the public spark release MUST have been built with the spark-hadoop-cloud module; the ASF ones
tend not to be.


Example: build with steve's s3a test config, hadoop trunk and cloudera spark build.

```bash

# Build locally
mi  -Phadoop-trunk   -Pspark-cdpd-master
 mvt -Dcloud.test.configuration.file=/Users/stevel/Projects/sparkwork/cloud-test-configs/s3a.xml  -Phadoop-trunk   -Pspark-cdpd-master
 ```
