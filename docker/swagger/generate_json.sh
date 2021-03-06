#!/usr/bin/env bash
#
# Copyright 2019 is-land
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


#  generate urls json
cd /usr/share/nginx/html
yamlsDir="./yamls/"
arr=( $(ls ./yamls/) )
urls="["
for i in "${!arr[@]}"
do
	 if [ "$i" == 0 ];then
	  urls="$urls{\"url\":\"$yamlsDir${arr[i]}\",\"name\":\"${arr[i]%.*}\"}"
	 else
	  urls="$urls,{\"url\":\"$yamlsDir${arr[i]}\",\"name\":\"${arr[i]%.*}\"}"
	 fi
done
urls="$urls]"
echo $urls > urls.json
