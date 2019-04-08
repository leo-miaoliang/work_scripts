#!/bin/bash
##################################################
date=$1
workspace=$2
url='http://files.51uuabc.com/tmk/mac_addr_data/mac_addr/'
filename=`curl $url$date'/' |grep "tgz" |awk -F '"' '{print $2}'| sed -e 's/[[:space:]]//g'`

if [ ! -n "$filename" ]; then
   echo "the file does not exists"
   exit 1
else
   mkdir -p  $workspace'/'$date'/'
   for i in ${filename}
   do
      cd $workspace'/'$date'/'
      curl -O $url$date'/'$i
      file=`basename $i .tgz`
      folder=$file'/'
      echo $folder
      if [ ! -d $folder ];then
           mkdir -p $folder;
      else
           echo 'the folder already exists';
      fi
      tar -zxvf $i -C $folder
      # repalace end line '{' with the etl_path
      for file in `find $folder -type f -regex ".*/[0-9]*"`
      do
         etl_path=',"etl_path":"'${file}'"}'
         sed -i "s%}$%${etl_path}%" $file
      done
   done
fi
# merge
cd $workspace'/'$date'/'
find . -type f -regex ".*/[0-9]*" -exec 'cat' {} \; > final_jsonfile

