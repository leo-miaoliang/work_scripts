url='http://files.51uuabc.com/tmk/mac_addr_data/output/{{ macros.ds(ti) }}/mac.csv'
        status_code=$(curl --write-out %{http_code} --silent --output /dev/null $url)
        if [[ "$status_code" -eq 200 ]] ; then
            mkdir -p {{ macros.ws(dag_run) }}'/'
            wget $url -O {{ macros.ws(dag_run) }}'/macfile.csv'
            exit 0
        fi


# Airflow script2
ws='{{ macros.ws(dag_run) }}/mac_addr/'
        url='http://files.51uuabc.com/tmk/mac_addr_data/mac_addr/{{ macros.ds(ti) }}/'
        files=`curl $url | grep "tgz" | awk -F '"' '{print $2}' | sed -e 's/[[:space:]]//g'`

        if [  -n "$files" ]; then
            mkdir -p  $ws
            cd $ws

            # download
            for gzfile in ${files}
            do
                curl -O $url$gzfile
                tar -zxvf $gzfile
            done
            # merge
            cd ..
            find . -type f -regex ".*/[0-9]*" -exec 'cat' {} \; > final_json
        fi