# Makefile for Spark WordCount project.

# Customize these paths for your environment.
# -----------------------------------------------------------
spark.root=/home/hduser/Desktop/spark/spark-2.3.1-bin-without-hadoop/
hadoop.root=/usr/local/hadoop
app.name=twitter followers count
jar.name=follow.jar
maven.jar.name=follow-0.0.1-SNAPSHOT.jar
job.max=50000
job.rs.name=tw.RSJoin
job.ms.name=tw.RepJoin
job.dset.name=tw.DSET
job.rdda.name=tw.RDDA
job.rddf.name=tw.RDDF
job.rddr.name=tw.RDDR
job.rddg.name=tw.RDDG
local.master=local[4]
local.input=input
local.output=output
# Pseudo-Cluster Execution
hdfs.user.name=hduser
hdfs.input=input
hdfs.output=output
# AWS EMR Execution
aws.emr.release=emr-5.17.0
aws.bucket.name=reducesidespark
aws.input=input
aws.output=output
aws.log.dir=log
aws.num.nodes=5
aws.instance.type=m4.large
aws.subnet.id=subnet-6ffe8660
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package
	cp target/${maven.jar.name} ${jar.name}

# Removes local output directory.
clean-local-output:
	rm -rf ${local.output}*

# Runs standalone
localRS: jar clean-local-output
	${spark.root}/bin/spark-submit --class ${job.rs.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input} ${local.output} ${job.max}
# Runs standalone
localMS: jar clean-local-output
	${spark.root}/bin/spark-submit --class ${job.ms.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input} ${local.output} ${job.max}
# Runs standalone
localRDDA: jar clean-local-output
	${spark.root}/bin/spark-submit --class ${job.rdda.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input} ${local.output} 
# Runs standalone
localRDDF: jar clean-local-output
	${spark.root}/bin/spark-submit --class ${job.rddf.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input} ${local.output} 
# Runs standalone
localRDDR: jar clean-local-output
	${spark.root}/bin/spark-submit --class ${job.rddr.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input} ${local.output} 
# Runs standalone
localRDDG: jar clean-local-output
	${spark.root}/bin/spark-submit --class ${job.rddg.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input} ${local.output}
# Runs standalone
localDSET: jar clean-local-output
	${spark.root}/bin/spark-submit --class ${job.dset.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input} ${local.output}

# Start HDFS
start-hdfs:
	${hadoop.root}/sbin/start-dfs.sh

# Stop HDFS
stop-hdfs: 
	${hadoop.root}/sbin/stop-dfs.sh
	
# Start YARN
start-yarn: stop-yarn
	${hadoop.root}/sbin/start-yarn.sh

# Stop YARN
stop-yarn:
	${hadoop.root}/sbin/stop-yarn.sh

# Reformats & initializes HDFS.
format-hdfs: stop-hdfs
	rm -rf /tmp/hadoop*
	${hadoop.root}/bin/hdfs namenode -format

# Initializes user & input directories of HDFS.	
init-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -rm -r -f /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}/${hdfs.input}

# Load data to HDFS
upload-input-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -put ${local.input}/* /user/${hdfs.user.name}/${hdfs.input}

# Removes hdfs output directory.
clean-hdfs-output:
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.output}*

# Download output from HDFS to local.
download-output-hdfs:
	mkdir ${local.output}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.output}/* ${local.output}

# Runs pseudo-clustered (ALL). ONLY RUN THIS ONCE, THEN USE: make pseudoq
# Make sure Hadoop  is set up (in /etc/hadoop files) for pseudo-clustered operation (not standalone).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation
pseudoRS: jar stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output 
	${spark.root}/bin/spark-submit --class ${job.rs.name} --master yarn --deploy-mode cluster ${jar.name} ${local.input} ${local.output} ${job.max}
	make download-output-hdfs

pseudoMS: jar stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output 
	${spark.root}/bin/spark-submit --class ${job.ms.name} --master yarn --deploy-mode cluster ${jar.name} ${local.input} ${local.output} ${job.max}
	make download-output-hdfs

# Runs pseudo-clustered (quickie).
pseudoq: jar clean-local-output clean-hdfs-output 
	${spark.root}/bin/spark-submit --class ${job.name} --master yarn --deploy-mode cluster ${jar.name} ${local.input} ${local.output}
	make download-output-hdfs

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}
	
# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}*"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${jar.name} s3://${aws.bucket.name}

# Main EMR launch.
awsRS: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "reduceiSide Join spark" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop Name=Spark \
		--steps Type=CUSTOM_JAR,Name="${app.name}",Jar="command-runner.jar",ActionOnFailure=TERMINATE_CLUSTER,Args=["spark-submit","--deploy-mode","cluster","--class","${job.rs.name}","s3://${aws.bucket.name}/${jar.name}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}","${job.max}"] \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--ec2-attributes SubnetId=${aws.subnet.id} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate
# Main EMR launch.
awsMS: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "reduceiSide Join spark" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop Name=Spark \
		--steps Type=CUSTOM_JAR,Name="${app.name}",Jar="command-runner.jar",ActionOnFailure=TERMINATE_CLUSTER,Args=["spark-submit","--deploy-mode","cluster","--class","${job.ms.name}","s3://${aws.bucket.name}/${jar.name}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}","${job.max}"] \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--ec2-attributes SubnetId=${aws.subnet.id} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate
		
# Download output from S3.
download-output-aws: clean-local-output
	mkdir ${local.output}
	aws s3 sync s3://${aws.bucket.name}/${aws.output} ${local.output}

# Change to standalone mode.
switch-standalone:
	cp config/standalone/*.xml ${hadoop.root}/etc/hadoop

# Change to pseudo-cluster mode.
switch-pseudo:
	cp config/pseudo/*.xml ${hadoop.root}/etc/hadoop

# Package for release.
distro:
	rm -f Spark-Demo.tar.gz
	rm -f Spark-Demo.zip
	rm -rf build
	mkdir -p build/deliv/Spark-Demo
	cp -r src build/deliv/Spark-Demo
	cp -r config build/deliv/Spark-Demo
	cp -r input build/deliv/Spark-Demo
	cp pom.xml build/deliv/Spark-Demo
	cp Makefile build/deliv/Spark-Demo
	cp README.txt build/deliv/Spark-Demo
	tar -czf Spark-Demo.tar.gz -C build/deliv Spark-Demo
	cd build/deliv && zip -rq ../../Spark-Demo.zip Spark-Demo
	