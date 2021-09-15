#!/bin/bash
####################  变量配置说明  #############################
table="/JSWJ/DB/MR_OPERATIONRECORD/"         #HDFS文件路径，定位到表
hospital="ZDYY"                              #医院名称，来源于table路径下一级子目录，比如：有医院 'ZDYY'
#position=27                                  #所要修改的字段是整个数据行记录中的第几列，比如：organ_code是MR_OPERATIONRECORD的第27列
nValue="ook"                                 #所要修改的字段的新值，比如把organ_code修改为"ook"
log="./dataHandler.log"                      #日志记录文件，建议以后台运行方式启动本sh文件，tail -f 查看日志文件
tmp="./tmp/"                                 #临时文件目录，此文件目录下会在任务执行前清空，需要为此目录留出一个月数据大小的空间，字符最后需加"/"
ignoreFile="_SUCCESS"                        #在MDB文件目录下的数据中有"_SUCCESS"文件，因不需要对此文件做任务操作，在此需要显示指定所需要过滤不处理的文件，目前仅支持单个 
positionpath="/opt/dataHandle/position.txt"  #position维护列表，e.g.: MR_OPERATIONRECORD=27   
#  生产测试校验步骤建议：
#     1、通过hdfs命令把生产数据copy到测试目录下，比如/TEST1/03/MR_OPERATIONRECORD/ZDYY/2018/~~~  /TEST1/03/MR_OPERATIONRECORD/ZDYY/2019/~~~
#    2、position值必须准确，此值为快速寻找organ_code在文件数据中每行的第几列，以便通过sed流处理快速定位
#    3、nVlaue值由业务自主设置
#    4、log、tmp建议全路径 
#     5、执行本shell过程中可通过日志查看进度
#     6、整体执行完成后，或根据日志进度，可通过hdfs命令查看数据是否修改准确     
#####################################################################

position=$(cat $positionpath | grep $(echo -e ${talbe%?} | awk -F "/" '{print $NF}' ) | awk -F "=" '{print $NF}')
if [[ ! -n $position ]] || [[ $position -lt 1 ]];then
   echo "Var position get value is fail!!!" >> $log
   exit 1;
fi

mkdir -p $tmp
chmod 777 -R $tmp
rm -rf $tmp*
basePath=$table$hospital
echo "$(date "+%Y-%m-%d %H:%M:%S") ~~ Hadoop HDFS data handle task '$basePath' start ..." >> $log
yearArr=($(hadoop fs -ls $basePath | awk '{print $8}' | tail -n+2))
for (( i=0;i<${#yearArr[@]};i++ )); do
	monthArr=($(hadoop fs -ls ${yearArr[i]} | awk '{print $8}' | tail -n +2 ))
	for (( j=0;j<${#monthArr[@]};j++ )); do
		hadoop fs -copyToLocal ${monthArr[j]}/* $tmp
		rm -rf $tmp$ignoreFile
		fileArr=($(ls $tmp))
		for (( k=0;k<${#fileArr[@]};k++ )); do
		  {
			fileN=$tmp$(echo "${fileArr[k]}" | awk -F "/" '{print $NF}')
			oData=$(sed 's/\x1f/\x23/g' $file | awk -v p=$position 'BEGIN{FS="#";OPS=" "} {print $p}')
			arr=(${oData//\n/})
			oV=${arr[0]}
			nS=1
			nE=1
			for (( y=1;y<${#arr[@]};y++ )); do
				if [ ${arr[y]} == $oV ]
				then
					let nE++
				else
					sed -i "${nS},${nE}s/\(.*\)$oV\(.*\)/\1$nValue\2/g" $file
					oV=${arr[y]}
					let nS=y+1  #nS=$($nS+i)
					let nE=nS
				fi 
			done
			sed -i "${nS},${nE}s/\(.*\)$oV\(.*\)/\1$nValue\2/g" $file 
		  } &
		  wait
		done 
		hadoop fs -put -f $tmp* ${monthArr[j]}
       rm -rf $tmp*
        echo "$(date "+%Y-%m-%d %H:%M:%S") ~~ Finish ${monthArr[j]} data handle done." >> $log
	done
done
