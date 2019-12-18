#!/bin/bash

#./run_notebook_bg.sh apache_toree_scala /home/jovyan/work/hadoop-client-scala-prod/local11_neo4j_connection_count.ipynb

# docker exec --user jovyan -d hadoop-client-scala sh -c 'nohup jupyter nbconvert --ExecutePreprocessor.timeout=-1 --CodeFoldingPreprocessor.remove_folded_code=False --ExecutePreprocessor.allow_errors=True --ExecutePreprocessor.kernel_name=apache_toree_scala --execute --to notebook --inplace /home/jovyan/work/hadoop-client-scala-prod/local11_neo4j_connection_count.ipynb > /home/jovyan/work/hadoop-client-scala-prod/sdout.log 2> /home/jovyan/work/hadoop-client-scala-prod/sderr.log'


docker_img=hadoop-client-scala
sdout_path=/home/jovyan/work/hadoop-client-scala-prod/sdout.log
sderr_path=/home/jovyan/work/hadoop-client-scala-prod/sderr.log

before=`pidof java`
echo $before

# doesnt work
bash -c "docker exec --user jovyan -d $docker_img sh -c 'nohup jupyter nbconvert --ExecutePreprocessor.timeout=-1 --CodeFoldingPreprocessor.remove_folded_code=False --ExecutePreprocessor.allow_errors=True --ExecutePreprocessor.kernel_name=$1 --execute --to notebook --inplace $2 > $sdout_path 2> $sderr_path'"

echo "Docker Image: $docker_img"
echo "Using kernel: $1"
echo "Running file: $2"
echo "stdout: $sdout_path"
echo "stderr: $sderr_path"

while [ 1 ]
do
    after=`pidof java`
    echo $after
    # str diff
    new_java_pid=`comm -23 <(tr ' ' $'\n' <<< $after | sort) <(tr ' ' $'\n' <<< $before | sort)`

    if [ -z "$new_java_pid" ]
    then
        echo "sleep 5"
        sleep 5
    else
        echo "The pidof java is: $new_java_pid"
        break
    fi
done

echo "sleep 5 ... just to be sure"
sleep 5
echo "starting ./cpu_mem_log.sh $new_java_pid"
nohup ./cpu_mem_log.sh $new_java_pid &
