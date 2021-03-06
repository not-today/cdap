#!/usr/bin/env bash

#
# Copyright © 2017 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`
script=`basename $0`

auth_token=
auth_file=
verbose=false
delete=false
verifyssl=true

function get_auth_file() {
  local __auth_file __uri_host __tmp
  # extract __uri_host by trimming the protocol and port from ${uri}
  __tmp=${uri##http*://}
  __uri_host=${__tmp%%\:[0-9]*}
  # find a matching access token
  for __auth_file in $(ls -a1 $HOME | grep ^\.cdap.accesstoken*) ; do
    if [[ .cdap.accesstoken.${__uri_host} == ${__auth_file}* ]]; then
      auth_file="$HOME/${__auth_file}"
      break
    fi
  done
}

function get_auth_token() {
  if [ -f "$auth_file" ]; then
    auth_token=$(cat $auth_file | sed 's/.*value\":\"\([^"]*\)\".*/\1/')
  fi
}

function usage() {
  echo "Tool for adding users to the profiles table. For authentication-enabled clusters, an"
  echo "access token must already have been generated by logging in via the CDAP CLI"
  echo "Usage: $script [--uri <uri>] [--namespace <namespace>] [--delete]"
  echo ""
  echo "  Options"
  echo "    --uri        Specifies the uri that CDAP is running on. (Default: http://localhost:11015)"
  echo "    --host       (deprecated, use --uri instead) Specifies the host that CDAP is running on."
  echo "    --namespace  Specifies the CDAP namespace to use. (Default: default)"
  echo "    --delete     Delete the users instead of adding them"
  echo "    --verbose    Print some information"
  echo "    --verify-ssl Validate CDAP Router SSL certificate. (Default: true)"
  echo "    --help       This help message"
  echo ""
}

uri="http://localhost:11015"
namespace="default"
stream="events"
while [ $# -gt 0 ]
do
  case "$1" in
    --host) shift; uri="http://${1}:11015"; shift;;
    --uri) shift; uri="${1}"; shift;;
    --namespace) shift; namespace="$1"; shift;;
    --verbose) shift; verbose=true;;
    --verify-ssl) shift; verifyssl="$1"; shift;;
    --delete) shift; delete=true;;
    *)  usage; exit 1
   esac
done

# pass -k to curl if --verify-ssl = false
if [ "${verifyssl}" == "false" ]; then
  ssl_opt="-k"
fi

# get the access token file
get_auth_file

#  get the access token
get_auth_token

OLD_IFS=IFS
IFS=$'\n'
lines=`cat "$bin"/../resources/users.txt`
for line in $lines
do
  userid=`echo $line | awk -F\" '{ print $4 }'`
  if [ $delete == "true" ]; then
    if [ $verbose == "true" ]; then
      echo Deleting user id: $userid
    fi
    status=`curl -qSfsw "%{http_code}\\n" ${ssl_opt} -H "Authorization: Bearer $auth_token" -X DELETE \
      ${uri}/v3/namespaces/${namespace}/apps/UserProfiles/services/UserProfileService/methods/profiles/${userid}`
    expected=200;
  else
    if [ $verbose == "true" ]; then
      echo Creating user id: $userid
    fi
    status=`curl -qSfsw "%{http_code}\\n" ${ssl_opt} -H "Authorization: Bearer $auth_token" -X PUT -d "$line" \
      ${uri}/v3/namespaces/${namespace}/apps/UserProfiles/services/UserProfileService/methods/profiles/${userid}`
    expected=201;
  fi
  if [ $status -ne $expected ]; then
    echo "Failed to send data."
    if [ $status == 401 ]; then
      if [ "x$auth_token" == "x" ]; then
        echo "No access token provided"
      else
        echo "Invalid access token"
      fi
    fi
    echo "Exiting program..."
    exit 1;
  fi
done
IFS=$OLD_IFS
