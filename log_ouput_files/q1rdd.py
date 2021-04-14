Script started on Sun 14 Mar 2021 05:27:55 AM EET
]0;user@master: ~/ergasia/meros1user@master:~/ergasia/meros1$ spark-submit q2sql.py PCPCP[Kspark-submit q2sql.py PCPC3[C[C[C[C[C[C[C[Cfg[Kspark-submit q3sql.py Cfg[Kspark-submit q3sql.py C2[C[C[C[C[C[C[C[CPCP[Ks[Kspark-submit q2sql.py P[1P[1@1[C[C[C[1P[1P[1P[1@r[1@d[1@d
21/03/14 05:28:17 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
21/03/14 05:28:17 INFO spark.SparkContext: Running Spark version 2.4.4
21/03/14 05:28:17 INFO spark.SparkContext: Submitted application: q1_rdd
21/03/14 05:28:17 INFO spark.SecurityManager: Changing view acls to: user
21/03/14 05:28:17 INFO spark.SecurityManager: Changing modify acls to: user
21/03/14 05:28:17 INFO spark.SecurityManager: Changing view acls groups to: 
21/03/14 05:28:17 INFO spark.SecurityManager: Changing modify acls groups to: 
21/03/14 05:28:17 INFO spark.SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(user); groups with view permissions: Set(); users  with modify permissions: Set(user); groups with modify permissions: Set()
21/03/14 05:28:18 INFO util.Utils: Successfully started service 'sparkDriver' on port 46119.
21/03/14 05:28:18 INFO spark.SparkEnv: Registering MapOutputTracker
21/03/14 05:28:18 INFO spark.SparkEnv: Registering BlockManagerMaster
21/03/14 05:28:18 INFO storage.BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
21/03/14 05:28:18 INFO storage.BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
21/03/14 05:28:18 INFO storage.DiskBlockManager: Created local directory at /tmp/blockmgr-ba3c8d75-cbbb-45e4-8482-ca8fecc833a2
21/03/14 05:28:18 INFO memory.MemoryStore: MemoryStore started with capacity 93.3 MB
21/03/14 05:28:18 INFO spark.SparkEnv: Registering OutputCommitCoordinator
21/03/14 05:28:18 INFO util.log: Logging initialized @2545ms
21/03/14 05:28:18 INFO server.Server: jetty-9.3.z-SNAPSHOT, build timestamp: unknown, git hash: unknown
21/03/14 05:28:18 INFO server.Server: Started @2619ms
21/03/14 05:28:18 INFO server.AbstractConnector: Started ServerConnector@61fd2d3d{HTTP/1.1,[http/1.1]}{0.0.0.0:4040}
21/03/14 05:28:18 INFO util.Utils: Successfully started service 'SparkUI' on port 4040.
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@213e5969{/jobs,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@2cfd861f{/jobs/json,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@5b9d2b29{/jobs/job,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@7e2f6ce6{/jobs/job/json,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@3ea2c042{/stages,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@c5470dc{/stages/json,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@2e2288e9{/stages/stage,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@2c1aae48{/stages/stage/json,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@16c51ee2{/stages/pool,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@41318e9c{/stages/pool/json,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@7d89befe{/storage,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@49827e9{/storage/json,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@3a940b96{/storage/rdd,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@771c3736{/storage/rdd/json,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@47d20965{/environment,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@230d919{/environment/json,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@18e2ba72{/executors,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@ed18bdf{/executors/json,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@591f3241{/executors/threadDump,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@5de3d06a{/executors/threadDump/json,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@505fc08e{/static,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@84e75cc{/,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@25d61900{/api,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@38eec722{/jobs/job/kill,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@9177ac8{/stages/stage/kill,null,AVAILABLE,@Spark}
21/03/14 05:28:18 INFO ui.SparkUI: Bound SparkUI to 0.0.0.0, and started at http://master:4040
21/03/14 05:28:18 INFO client.StandaloneAppClient$ClientEndpoint: Connecting to master spark://master:7077...
21/03/14 05:28:18 INFO client.TransportClientFactory: Successfully created connection to master/192.168.0.1:7077 after 36 ms (0 ms spent in bootstraps)
21/03/14 05:28:18 INFO cluster.StandaloneSchedulerBackend: Connected to Spark cluster with app ID app-20210314052818-0097
21/03/14 05:28:18 INFO client.StandaloneAppClient$ClientEndpoint: Executor added: app-20210314052818-0097/0 on worker-20210313132157-192.168.0.1-43826 (192.168.0.1:43826) with 2 core(s)
21/03/14 05:28:18 INFO cluster.StandaloneSchedulerBackend: Granted executor ID app-20210314052818-0097/0 on hostPort 192.168.0.1:43826 with 2 core(s), 3.0 GB RAM
21/03/14 05:28:18 INFO client.StandaloneAppClient$ClientEndpoint: Executor added: app-20210314052818-0097/1 on worker-20210313132158-192.168.0.2-46469 (192.168.0.2:46469) with 2 core(s)
21/03/14 05:28:18 INFO cluster.StandaloneSchedulerBackend: Granted executor ID app-20210314052818-0097/1 on hostPort 192.168.0.2:46469 with 2 core(s), 3.0 GB RAM
21/03/14 05:28:18 INFO util.Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 41729.
21/03/14 05:28:18 INFO netty.NettyBlockTransferService: Server created on master:41729
21/03/14 05:28:18 INFO storage.BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
21/03/14 05:28:18 INFO client.StandaloneAppClient$ClientEndpoint: Executor updated: app-20210314052818-0097/1 is now RUNNING
21/03/14 05:28:18 INFO client.StandaloneAppClient$ClientEndpoint: Executor updated: app-20210314052818-0097/0 is now RUNNING
21/03/14 05:28:18 INFO storage.BlockManagerMaster: Registering BlockManager BlockManagerId(driver, master, 41729, None)
21/03/14 05:28:18 INFO storage.BlockManagerMasterEndpoint: Registering block manager master:41729 with 93.3 MB RAM, BlockManagerId(driver, master, 41729, None)
21/03/14 05:28:18 INFO storage.BlockManagerMaster: Registered BlockManager BlockManagerId(driver, master, 41729, None)
21/03/14 05:28:18 INFO storage.BlockManager: Initialized BlockManager: BlockManagerId(driver, master, 41729, None)
21/03/14 05:28:19 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@521ba693{/metrics/json,null,AVAILABLE,@Spark}
21/03/14 05:28:19 INFO cluster.StandaloneSchedulerBackend: SchedulerBackend is ready for scheduling beginning after reached minRegisteredResourcesRatio: 0.0
21/03/14 05:28:19 INFO internal.SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir ('file:/home/user/ergasia/meros1/spark-warehouse/').
21/03/14 05:28:19 INFO internal.SharedState: Warehouse path is 'file:/home/user/ergasia/meros1/spark-warehouse/'.
21/03/14 05:28:19 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@62712e36{/SQL,null,AVAILABLE,@Spark}
21/03/14 05:28:19 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@20ff9a01{/SQL/json,null,AVAILABLE,@Spark}
21/03/14 05:28:19 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@8b442dd{/SQL/execution,null,AVAILABLE,@Spark}
21/03/14 05:28:19 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@777baea8{/SQL/execution/json,null,AVAILABLE,@Spark}
21/03/14 05:28:19 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@119c009a{/static/sql,null,AVAILABLE,@Spark}
21/03/14 05:28:20 INFO state.StateStoreCoordinatorRef: Registered StateStoreCoordinator endpoint
21/03/14 05:28:21 INFO cluster.CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (192.168.0.2:51632) with ID 1
21/03/14 05:28:21 INFO storage.BlockManagerMasterEndpoint: Registering block manager 192.168.0.2:39163 with 1458.6 MB RAM, BlockManagerId(1, 192.168.0.2, 39163, None)
21/03/14 05:28:21 INFO memory.MemoryStore: Block broadcast_0 stored as values in memory (estimated size 239.4 KB, free 93.1 MB)
21/03/14 05:28:21 INFO memory.MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 23.1 KB, free 93.0 MB)
21/03/14 05:28:21 INFO storage.BlockManagerInfo: Added broadcast_0_piece0 in memory on master:41729 (size: 23.1 KB, free: 93.3 MB)
21/03/14 05:28:21 INFO spark.SparkContext: Created broadcast 0 from textFile at NativeMethodAccessorImpl.java:0
21/03/14 05:28:22 INFO cluster.CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (192.168.0.1:49418) with ID 0
21/03/14 05:28:22 INFO mapred.FileInputFormat: Total input paths to process : 1
21/03/14 05:28:22 INFO storage.BlockManagerMasterEndpoint: Registering block manager 192.168.0.1:33835 with 1458.6 MB RAM, BlockManagerId(0, 192.168.0.1, 33835, None)
21/03/14 05:28:23 INFO spark.SparkContext: Starting job: sortByKey at /home/user/ergasia/meros1/q1rdd.py:57
21/03/14 05:28:23 INFO scheduler.DAGScheduler: Registering RDD 3 (reduceByKey at /home/user/ergasia/meros1/q1rdd.py:56)
21/03/14 05:28:23 INFO scheduler.DAGScheduler: Got job 0 (sortByKey at /home/user/ergasia/meros1/q1rdd.py:57) with 2 output partitions
21/03/14 05:28:23 INFO scheduler.DAGScheduler: Final stage: ResultStage 1 (sortByKey at /home/user/ergasia/meros1/q1rdd.py:57)
21/03/14 05:28:23 INFO scheduler.DAGScheduler: Parents of final stage: List(ShuffleMapStage 0)
21/03/14 05:28:23 INFO scheduler.DAGScheduler: Missing parents: List(ShuffleMapStage 0)
21/03/14 05:28:23 INFO scheduler.DAGScheduler: Submitting ShuffleMapStage 0 (PairwiseRDD[3] at reduceByKey at /home/user/ergasia/meros1/q1rdd.py:56), which has no missing parents
21/03/14 05:28:23 INFO memory.MemoryStore: Block broadcast_1 stored as values in memory (estimated size 12.9 KB, free 93.0 MB)
21/03/14 05:28:23 INFO memory.MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 8.4 KB, free 93.0 MB)
21/03/14 05:28:23 INFO storage.BlockManagerInfo: Added broadcast_1_piece0 in memory on master:41729 (size: 8.4 KB, free: 93.3 MB)
21/03/14 05:28:23 INFO spark.SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1161
21/03/14 05:28:23 INFO scheduler.DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 0 (PairwiseRDD[3] at reduceByKey at /home/user/ergasia/meros1/q1rdd.py:56) (first 15 tasks are for partitions Vector(0, 1))
21/03/14 05:28:23 INFO scheduler.TaskSchedulerImpl: Adding task set 0.0 with 2 tasks
21/03/14 05:28:23 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, 192.168.0.2, executor 1, partition 0, ANY, 7882 bytes)
21/03/14 05:28:23 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1, 192.168.0.1, executor 0, partition 1, ANY, 7882 bytes)
21/03/14 05:28:23 INFO storage.BlockManagerInfo: Added broadcast_1_piece0 in memory on 192.168.0.2:39163 (size: 8.4 KB, free: 1458.6 MB)
21/03/14 05:28:23 INFO storage.BlockManagerInfo: Added broadcast_1_piece0 in memory on 192.168.0.1:33835 (size: 8.4 KB, free: 1458.6 MB)
21/03/14 05:28:23 INFO storage.BlockManagerInfo: Added broadcast_0_piece0 in memory on 192.168.0.2:39163 (size: 23.1 KB, free: 1458.6 MB)
21/03/14 05:28:23 INFO storage.BlockManagerInfo: Added broadcast_0_piece0 in memory on 192.168.0.1:33835 (size: 23.1 KB, free: 1458.6 MB)
21/03/14 05:28:26 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 2919 ms on 192.168.0.2 (executor 1) (1/2)
21/03/14 05:28:26 INFO python.PythonAccumulatorV2: Connected to AccumulatorServer at host: 127.0.0.1 port: 60144
21/03/14 05:28:26 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 2947 ms on 192.168.0.1 (executor 0) (2/2)
21/03/14 05:28:26 INFO scheduler.DAGScheduler: ShuffleMapStage 0 (reduceByKey at /home/user/ergasia/meros1/q1rdd.py:56) finished in 3.042 s
21/03/14 05:28:26 INFO scheduler.DAGScheduler: looking for newly runnable stages
21/03/14 05:28:26 INFO scheduler.DAGScheduler: running: Set()
21/03/14 05:28:26 INFO scheduler.DAGScheduler: waiting: Set(ResultStage 1)
21/03/14 05:28:26 INFO scheduler.DAGScheduler: failed: Set()
21/03/14 05:28:26 INFO scheduler.DAGScheduler: Submitting ResultStage 1 (PythonRDD[6] at sortByKey at /home/user/ergasia/meros1/q1rdd.py:57), which has no missing parents
21/03/14 05:28:26 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
21/03/14 05:28:26 INFO memory.MemoryStore: Block broadcast_2 stored as values in memory (estimated size 9.0 KB, free 93.0 MB)
21/03/14 05:28:26 INFO memory.MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 5.8 KB, free 93.0 MB)
21/03/14 05:28:26 INFO storage.BlockManagerInfo: Added broadcast_2_piece0 in memory on master:41729 (size: 5.8 KB, free: 93.3 MB)
21/03/14 05:28:26 INFO spark.SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1161
21/03/14 05:28:26 INFO scheduler.DAGScheduler: Submitting 2 missing tasks from ResultStage 1 (PythonRDD[6] at sortByKey at /home/user/ergasia/meros1/q1rdd.py:57) (first 15 tasks are for partitions Vector(0, 1))
21/03/14 05:28:26 INFO scheduler.TaskSchedulerImpl: Adding task set 1.0 with 2 tasks
21/03/14 05:28:26 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 1.0 (TID 2, 192.168.0.1, executor 0, partition 0, NODE_LOCAL, 7666 bytes)
21/03/14 05:28:26 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 1.0 (TID 3, 192.168.0.2, executor 1, partition 1, NODE_LOCAL, 7666 bytes)
21/03/14 05:28:26 INFO storage.BlockManagerInfo: Added broadcast_2_piece0 in memory on 192.168.0.2:39163 (size: 5.8 KB, free: 1458.6 MB)
21/03/14 05:28:26 INFO storage.BlockManagerInfo: Added broadcast_2_piece0 in memory on 192.168.0.1:33835 (size: 5.8 KB, free: 1458.6 MB)
21/03/14 05:28:26 INFO spark.MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 0 to 192.168.0.1:49418
21/03/14 05:28:26 INFO spark.MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 0 to 192.168.0.2:51632
21/03/14 05:28:26 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 1.0 (TID 3) in 240 ms on 192.168.0.2 (executor 1) (1/2)
21/03/14 05:28:26 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 1.0 (TID 2) in 262 ms on 192.168.0.1 (executor 0) (2/2)
21/03/14 05:28:26 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
21/03/14 05:28:26 INFO scheduler.DAGScheduler: ResultStage 1 (sortByKey at /home/user/ergasia/meros1/q1rdd.py:57) finished in 0.279 s
21/03/14 05:28:26 INFO scheduler.DAGScheduler: Job 0 finished: sortByKey at /home/user/ergasia/meros1/q1rdd.py:57, took 3.408471 s
21/03/14 05:28:26 INFO spark.SparkContext: Starting job: sortByKey at /home/user/ergasia/meros1/q1rdd.py:57
21/03/14 05:28:26 INFO scheduler.DAGScheduler: Got job 1 (sortByKey at /home/user/ergasia/meros1/q1rdd.py:57) with 2 output partitions
21/03/14 05:28:26 INFO scheduler.DAGScheduler: Final stage: ResultStage 3 (sortByKey at /home/user/ergasia/meros1/q1rdd.py:57)
21/03/14 05:28:26 INFO scheduler.DAGScheduler: Parents of final stage: List(ShuffleMapStage 2)
21/03/14 05:28:26 INFO scheduler.DAGScheduler: Missing parents: List()
21/03/14 05:28:26 INFO scheduler.DAGScheduler: Submitting ResultStage 3 (PythonRDD[7] at sortByKey at /home/user/ergasia/meros1/q1rdd.py:57), which has no missing parents
21/03/14 05:28:26 INFO memory.MemoryStore: Block broadcast_3 stored as values in memory (estimated size 9.0 KB, free 93.0 MB)
21/03/14 05:28:26 INFO memory.MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 5.8 KB, free 93.0 MB)
21/03/14 05:28:26 INFO storage.BlockManagerInfo: Added broadcast_3_piece0 in memory on master:41729 (size: 5.8 KB, free: 93.3 MB)
21/03/14 05:28:26 INFO spark.SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1161
21/03/14 05:28:26 INFO scheduler.DAGScheduler: Submitting 2 missing tasks from ResultStage 3 (PythonRDD[7] at sortByKey at /home/user/ergasia/meros1/q1rdd.py:57) (first 15 tasks are for partitions Vector(0, 1))
21/03/14 05:28:26 INFO scheduler.TaskSchedulerImpl: Adding task set 3.0 with 2 tasks
21/03/14 05:28:26 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 3.0 (TID 4, 192.168.0.1, executor 0, partition 0, NODE_LOCAL, 7666 bytes)
21/03/14 05:28:26 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 3.0 (TID 5, 192.168.0.2, executor 1, partition 1, NODE_LOCAL, 7666 bytes)
21/03/14 05:28:26 INFO storage.BlockManagerInfo: Added broadcast_3_piece0 in memory on 192.168.0.1:33835 (size: 5.8 KB, free: 1458.6 MB)
21/03/14 05:28:26 INFO storage.BlockManagerInfo: Added broadcast_3_piece0 in memory on 192.168.0.2:39163 (size: 5.8 KB, free: 1458.6 MB)
21/03/14 05:28:26 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 3.0 (TID 5) in 106 ms on 192.168.0.2 (executor 1) (1/2)
21/03/14 05:28:26 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 3.0 (TID 4) in 125 ms on 192.168.0.1 (executor 0) (2/2)
21/03/14 05:28:26 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 3.0, whose tasks have all completed, from pool 
21/03/14 05:28:26 INFO scheduler.DAGScheduler: ResultStage 3 (sortByKey at /home/user/ergasia/meros1/q1rdd.py:57) finished in 0.135 s
21/03/14 05:28:26 INFO scheduler.DAGScheduler: Job 1 finished: sortByKey at /home/user/ergasia/meros1/q1rdd.py:57, took 0.142914 s
21/03/14 05:28:26 INFO spark.SparkContext: Starting job: collect at /home/user/ergasia/meros1/q1rdd.py:57
21/03/14 05:28:26 INFO scheduler.DAGScheduler: Registering RDD 9 (sortByKey at /home/user/ergasia/meros1/q1rdd.py:57)
21/03/14 05:28:26 INFO scheduler.DAGScheduler: Got job 2 (collect at /home/user/ergasia/meros1/q1rdd.py:57) with 2 output partitions
21/03/14 05:28:26 INFO scheduler.DAGScheduler: Final stage: ResultStage 6 (collect at /home/user/ergasia/meros1/q1rdd.py:57)
21/03/14 05:28:26 INFO scheduler.DAGScheduler: Parents of final stage: List(ShuffleMapStage 5)
21/03/14 05:28:26 INFO scheduler.DAGScheduler: Missing parents: List(ShuffleMapStage 5)
21/03/14 05:28:26 INFO scheduler.DAGScheduler: Submitting ShuffleMapStage 5 (PairwiseRDD[9] at sortByKey at /home/user/ergasia/meros1/q1rdd.py:57), which has no missing parents
21/03/14 05:28:26 INFO memory.MemoryStore: Block broadcast_4 stored as values in memory (estimated size 9.6 KB, free 93.0 MB)
21/03/14 05:28:26 INFO memory.MemoryStore: Block broadcast_4_piece0 stored as bytes in memory (estimated size 6.3 KB, free 93.0 MB)
21/03/14 05:28:26 INFO storage.BlockManagerInfo: Added broadcast_4_piece0 in memory on master:41729 (size: 6.3 KB, free: 93.3 MB)
21/03/14 05:28:26 INFO spark.SparkContext: Created broadcast 4 from broadcast at DAGScheduler.scala:1161
21/03/14 05:28:26 INFO scheduler.DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 5 (PairwiseRDD[9] at sortByKey at /home/user/ergasia/meros1/q1rdd.py:57) (first 15 tasks are for partitions Vector(0, 1))
21/03/14 05:28:26 INFO scheduler.TaskSchedulerImpl: Adding task set 5.0 with 2 tasks
21/03/14 05:28:26 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 5.0 (TID 6, 192.168.0.1, executor 0, partition 0, NODE_LOCAL, 7655 bytes)
21/03/14 05:28:26 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 5.0 (TID 7, 192.168.0.2, executor 1, partition 1, NODE_LOCAL, 7655 bytes)
21/03/14 05:28:26 INFO storage.BlockManagerInfo: Added broadcast_4_piece0 in memory on 192.168.0.2:39163 (size: 6.3 KB, free: 1458.6 MB)
21/03/14 05:28:26 INFO storage.BlockManagerInfo: Added broadcast_4_piece0 in memory on 192.168.0.1:33835 (size: 6.3 KB, free: 1458.6 MB)
21/03/14 05:28:26 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 5.0 (TID 7) in 112 ms on 192.168.0.2 (executor 1) (1/2)
21/03/14 05:28:26 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 5.0 (TID 6) in 133 ms on 192.168.0.1 (executor 0) (2/2)
21/03/14 05:28:26 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 5.0, whose tasks have all completed, from pool 
21/03/14 05:28:26 INFO scheduler.DAGScheduler: ShuffleMapStage 5 (sortByKey at /home/user/ergasia/meros1/q1rdd.py:57) finished in 0.145 s
21/03/14 05:28:26 INFO scheduler.DAGScheduler: looking for newly runnable stages
21/03/14 05:28:26 INFO scheduler.DAGScheduler: running: Set()
21/03/14 05:28:26 INFO scheduler.DAGScheduler: waiting: Set(ResultStage 6)
21/03/14 05:28:26 INFO scheduler.DAGScheduler: failed: Set()
21/03/14 05:28:26 INFO scheduler.DAGScheduler: Submitting ResultStage 6 (PythonRDD[12] at collect at /home/user/ergasia/meros1/q1rdd.py:57), which has no missing parents
21/03/14 05:28:26 INFO memory.MemoryStore: Block broadcast_5 stored as values in memory (estimated size 6.6 KB, free 93.0 MB)
21/03/14 05:28:26 INFO memory.MemoryStore: Block broadcast_5_piece0 stored as bytes in memory (estimated size 4.2 KB, free 93.0 MB)
21/03/14 05:28:26 INFO storage.BlockManagerInfo: Added broadcast_5_piece0 in memory on master:41729 (size: 4.2 KB, free: 93.2 MB)
21/03/14 05:28:26 INFO spark.SparkContext: Created broadcast 5 from broadcast at DAGScheduler.scala:1161
21/03/14 05:28:26 INFO scheduler.DAGScheduler: Submitting 2 missing tasks from ResultStage 6 (PythonRDD[12] at collect at /home/user/ergasia/meros1/q1rdd.py:57) (first 15 tasks are for partitions Vector(0, 1))
21/03/14 05:28:26 INFO scheduler.TaskSchedulerImpl: Adding task set 6.0 with 2 tasks
21/03/14 05:28:26 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 6.0 (TID 8, 192.168.0.2, executor 1, partition 0, NODE_LOCAL, 7666 bytes)
21/03/14 05:28:26 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 6.0 (TID 9, 192.168.0.1, executor 0, partition 1, NODE_LOCAL, 7666 bytes)
21/03/14 05:28:26 INFO storage.BlockManagerInfo: Added broadcast_5_piece0 in memory on 192.168.0.1:33835 (size: 4.2 KB, free: 1458.5 MB)
21/03/14 05:28:26 INFO storage.BlockManagerInfo: Added broadcast_5_piece0 in memory on 192.168.0.2:39163 (size: 4.2 KB, free: 1458.5 MB)
21/03/14 05:28:26 INFO spark.MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 1 to 192.168.0.2:51632
21/03/14 05:28:26 INFO spark.MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 1 to 192.168.0.1:49418
21/03/14 05:28:26 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 6.0 (TID 8) in 94 ms on 192.168.0.2 (executor 1) (1/2)
21/03/14 05:28:26 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 6.0 (TID 9) in 107 ms on 192.168.0.1 (executor 0) (2/2)
21/03/14 05:28:26 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 6.0, whose tasks have all completed, from pool 
21/03/14 05:28:26 INFO scheduler.DAGScheduler: ResultStage 6 (collect at /home/user/ergasia/meros1/q1rdd.py:57) finished in 0.125 s
21/03/14 05:28:26 INFO scheduler.DAGScheduler: Job 2 finished: collect at /home/user/ergasia/meros1/q1rdd.py:57, took 0.283323 s
Year    |   Profit   |  Title 
(2000, (2100.0, 'Billy Elliot'))
(2001, (1732.8339666666668, '千と千尋の神隠し'))
(2002, (7274.880880000001, 'My Big Fat Greek Wedding'))
(2003, (532933.9449541284, 'Tarnation'))
(2004, (43861.65846153846, 'Super Size Me'))
(2005, (419747562.5, '웰컴 투 동막골'))
(2006, (10078.331, 'Facing the Giants'))
(2007, (1288938.6666666667, 'Paranormal Activity'))
(2008, (6591.2634, 'Fireproof'))
(2009, (3252.9411764705883, 'The Collector'))
(2010, (10053.143333333333, 'Catfish'))
(2011, (2688072.0430107526, 'From Prada to Nada'))
(2012, (275558300.0, 'Aquí Entre Nos'))
(2013, (99999900.0, 'Nurse 3-D'))
(2014, (8817.4335, 'The Quiet Ones'))
(2015, (221568.98, '대호'))
(2016, (2976.911088888889, 'Split'))
(2017, (15484.255, 'A Ghost Story'))
Time of Q1 using Map-Reduce with csv is: 6.2992103099823 seconds
21/03/14 05:28:26 INFO spark.SparkContext: Invoking stop() from shutdown hook
21/03/14 05:28:26 INFO server.AbstractConnector: Stopped Spark@61fd2d3d{HTTP/1.1,[http/1.1]}{0.0.0.0:4040}
21/03/14 05:28:26 INFO ui.SparkUI: Stopped Spark web UI at http://master:4040
21/03/14 05:28:26 INFO cluster.StandaloneSchedulerBackend: Shutting down all executors
21/03/14 05:28:26 INFO cluster.CoarseGrainedSchedulerBackend$DriverEndpoint: Asking each executor to shut down
21/03/14 05:28:26 INFO spark.MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
21/03/14 05:28:27 INFO memory.MemoryStore: MemoryStore cleared
21/03/14 05:28:27 INFO storage.BlockManager: BlockManager stopped
21/03/14 05:28:27 INFO storage.BlockManagerMaster: BlockManagerMaster stopped
21/03/14 05:28:27 INFO scheduler.OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
21/03/14 05:28:27 INFO spark.SparkContext: Successfully stopped SparkContext
21/03/14 05:28:27 INFO util.ShutdownHookManager: Shutdown hook called
21/03/14 05:28:27 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-b21fa268-fdc5-4f74-b1f2-1a00d5f17f28
21/03/14 05:28:27 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-9dcfaf33-eec6-4445-a861-fff605286abc
21/03/14 05:28:27 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-b21fa268-fdc5-4f74-b1f2-1a00d5f17f28/pyspark-520e4c42-a04b-4c48-bfa4-6741bc7aad52
]0

Script done on Sun 14 Mar 2021 05:28:41 AM EET
