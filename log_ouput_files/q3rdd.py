Script started on Sun 14 Mar 2021 05:31:28 AM EET
]0;user@master: ~/ergasia/meros1user@master:~/ergasia/meros1$ exitspark-submit q2rdd.py P[C[K[1P[1@3
21/03/14 05:31:36 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
21/03/14 05:31:36 INFO spark.SparkContext: Running Spark version 2.4.4
21/03/14 05:31:36 INFO spark.SparkContext: Submitted application: Q3_RDD
21/03/14 05:31:36 INFO spark.SecurityManager: Changing view acls to: user
21/03/14 05:31:36 INFO spark.SecurityManager: Changing modify acls to: user
21/03/14 05:31:36 INFO spark.SecurityManager: Changing view acls groups to: 
21/03/14 05:31:36 INFO spark.SecurityManager: Changing modify acls groups to: 
21/03/14 05:31:36 INFO spark.SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(user); groups with view permissions: Set(); users  with modify permissions: Set(user); groups with modify permissions: Set()
21/03/14 05:31:37 INFO util.Utils: Successfully started service 'sparkDriver' on port 39810.
21/03/14 05:31:37 INFO spark.SparkEnv: Registering MapOutputTracker
21/03/14 05:31:37 INFO spark.SparkEnv: Registering BlockManagerMaster
21/03/14 05:31:37 INFO storage.BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
21/03/14 05:31:37 INFO storage.BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
21/03/14 05:31:37 INFO storage.DiskBlockManager: Created local directory at /tmp/blockmgr-8634b04c-b6f0-4848-bc6a-4eb863c41c32
21/03/14 05:31:37 INFO memory.MemoryStore: MemoryStore started with capacity 93.3 MB
21/03/14 05:31:37 INFO spark.SparkEnv: Registering OutputCommitCoordinator
21/03/14 05:31:37 INFO util.log: Logging initialized @2648ms
21/03/14 05:31:37 INFO server.Server: jetty-9.3.z-SNAPSHOT, build timestamp: unknown, git hash: unknown
21/03/14 05:31:37 INFO server.Server: Started @2725ms
21/03/14 05:31:37 INFO server.AbstractConnector: Started ServerConnector@235c0abc{HTTP/1.1,[http/1.1]}{0.0.0.0:4040}
21/03/14 05:31:37 INFO util.Utils: Successfully started service 'SparkUI' on port 4040.
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@213e5969{/jobs,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@2cfd861f{/jobs/json,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@5b9d2b29{/jobs/job,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@7e2f6ce6{/jobs/job/json,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@3ea2c042{/stages,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@c5470dc{/stages/json,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@2e2288e9{/stages/stage,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@2c1aae48{/stages/stage/json,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@16c51ee2{/stages/pool,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@41318e9c{/stages/pool/json,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@7d89befe{/storage,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@49827e9{/storage/json,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@3a940b96{/storage/rdd,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@771c3736{/storage/rdd/json,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@47d20965{/environment,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@230d919{/environment/json,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@18e2ba72{/executors,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@ed18bdf{/executors/json,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@591f3241{/executors/threadDump,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@5de3d06a{/executors/threadDump/json,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@505fc08e{/static,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@84e75cc{/,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@25d61900{/api,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@38eec722{/jobs/job/kill,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@9177ac8{/stages/stage/kill,null,AVAILABLE,@Spark}
21/03/14 05:31:37 INFO ui.SparkUI: Bound SparkUI to 0.0.0.0, and started at http://master:4040
21/03/14 05:31:37 INFO client.StandaloneAppClient$ClientEndpoint: Connecting to master spark://master:7077...
21/03/14 05:31:37 INFO client.TransportClientFactory: Successfully created connection to master/192.168.0.1:7077 after 47 ms (0 ms spent in bootstraps)
21/03/14 05:31:37 INFO cluster.StandaloneSchedulerBackend: Connected to Spark cluster with app ID app-20210314053137-0099
21/03/14 05:31:37 INFO util.Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 37795.
21/03/14 05:31:37 INFO client.StandaloneAppClient$ClientEndpoint: Executor added: app-20210314053137-0099/0 on worker-20210313132157-192.168.0.1-43826 (192.168.0.1:43826) with 2 core(s)
21/03/14 05:31:37 INFO netty.NettyBlockTransferService: Server created on master:37795
21/03/14 05:31:37 INFO cluster.StandaloneSchedulerBackend: Granted executor ID app-20210314053137-0099/0 on hostPort 192.168.0.1:43826 with 2 core(s), 3.0 GB RAM
21/03/14 05:31:37 INFO storage.BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
21/03/14 05:31:37 INFO client.StandaloneAppClient$ClientEndpoint: Executor added: app-20210314053137-0099/1 on worker-20210313132158-192.168.0.2-46469 (192.168.0.2:46469) with 2 core(s)
21/03/14 05:31:37 INFO cluster.StandaloneSchedulerBackend: Granted executor ID app-20210314053137-0099/1 on hostPort 192.168.0.2:46469 with 2 core(s), 3.0 GB RAM
21/03/14 05:31:37 INFO client.StandaloneAppClient$ClientEndpoint: Executor updated: app-20210314053137-0099/1 is now RUNNING
21/03/14 05:31:37 INFO client.StandaloneAppClient$ClientEndpoint: Executor updated: app-20210314053137-0099/0 is now RUNNING
21/03/14 05:31:37 INFO storage.BlockManagerMaster: Registering BlockManager BlockManagerId(driver, master, 37795, None)
21/03/14 05:31:37 INFO storage.BlockManagerMasterEndpoint: Registering block manager master:37795 with 93.3 MB RAM, BlockManagerId(driver, master, 37795, None)
21/03/14 05:31:38 INFO storage.BlockManagerMaster: Registered BlockManager BlockManagerId(driver, master, 37795, None)
21/03/14 05:31:38 INFO storage.BlockManager: Initialized BlockManager: BlockManagerId(driver, master, 37795, None)
21/03/14 05:31:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@521ba693{/metrics/json,null,AVAILABLE,@Spark}
21/03/14 05:31:38 INFO cluster.StandaloneSchedulerBackend: SchedulerBackend is ready for scheduling beginning after reached minRegisteredResourcesRatio: 0.0
21/03/14 05:31:38 INFO internal.SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir ('file:/home/user/ergasia/meros1/spark-warehouse/').
21/03/14 05:31:38 INFO internal.SharedState: Warehouse path is 'file:/home/user/ergasia/meros1/spark-warehouse/'.
21/03/14 05:31:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@62712e36{/SQL,null,AVAILABLE,@Spark}
21/03/14 05:31:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@20ff9a01{/SQL/json,null,AVAILABLE,@Spark}
21/03/14 05:31:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@8b442dd{/SQL/execution,null,AVAILABLE,@Spark}
21/03/14 05:31:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@777baea8{/SQL/execution/json,null,AVAILABLE,@Spark}
21/03/14 05:31:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@119c009a{/static/sql,null,AVAILABLE,@Spark}
21/03/14 05:31:40 INFO state.StateStoreCoordinatorRef: Registered StateStoreCoordinator endpoint
21/03/14 05:31:40 INFO cluster.CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (192.168.0.2:43928) with ID 1
21/03/14 05:31:40 INFO storage.BlockManagerMasterEndpoint: Registering block manager 192.168.0.2:46470 with 1458.6 MB RAM, BlockManagerId(1, 192.168.0.2, 46470, None)
21/03/14 05:31:41 INFO memory.MemoryStore: Block broadcast_0 stored as values in memory (estimated size 239.4 KB, free 93.1 MB)
21/03/14 05:31:41 INFO memory.MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 23.1 KB, free 93.0 MB)
21/03/14 05:31:41 INFO storage.BlockManagerInfo: Added broadcast_0_piece0 in memory on master:37795 (size: 23.1 KB, free: 93.3 MB)
21/03/14 05:31:41 INFO spark.SparkContext: Created broadcast 0 from textFile at NativeMethodAccessorImpl.java:0
21/03/14 05:31:41 INFO cluster.CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (192.168.0.1:58384) with ID 0
21/03/14 05:31:42 INFO storage.BlockManagerMasterEndpoint: Registering block manager 192.168.0.1:44204 with 1458.6 MB RAM, BlockManagerId(0, 192.168.0.1, 44204, None)
21/03/14 05:31:42 INFO mapred.FileInputFormat: Total input paths to process : 1
21/03/14 05:31:42 INFO memory.MemoryStore: Block broadcast_1 stored as values in memory (estimated size 239.5 KB, free 92.8 MB)
21/03/14 05:31:42 INFO memory.MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 23.1 KB, free 92.8 MB)
21/03/14 05:31:42 INFO storage.BlockManagerInfo: Added broadcast_1_piece0 in memory on master:37795 (size: 23.1 KB, free: 93.3 MB)
21/03/14 05:31:42 INFO spark.SparkContext: Created broadcast 1 from textFile at NativeMethodAccessorImpl.java:0
21/03/14 05:31:42 INFO mapred.FileInputFormat: Total input paths to process : 1
21/03/14 05:31:42 INFO spark.SparkContext: Starting job: collect at /home/user/ergasia/meros1/q3rdd.py:31
21/03/14 05:31:42 INFO scheduler.DAGScheduler: Registering RDD 3 (reduceByKey at /home/user/ergasia/meros1/q3rdd.py:20)
21/03/14 05:31:42 INFO scheduler.DAGScheduler: Registering RDD 12 (join at /home/user/ergasia/meros1/q3rdd.py:28)
21/03/14 05:31:42 INFO scheduler.DAGScheduler: Registering RDD 16 (reduceByKey at /home/user/ergasia/meros1/q3rdd.py:30)
21/03/14 05:31:42 INFO scheduler.DAGScheduler: Got job 0 (collect at /home/user/ergasia/meros1/q3rdd.py:31) with 13 output partitions
21/03/14 05:31:42 INFO scheduler.DAGScheduler: Final stage: ResultStage 3 (collect at /home/user/ergasia/meros1/q3rdd.py:31)
21/03/14 05:31:42 INFO scheduler.DAGScheduler: Parents of final stage: List(ShuffleMapStage 2)
21/03/14 05:31:42 INFO scheduler.DAGScheduler: Missing parents: List(ShuffleMapStage 2)
21/03/14 05:31:42 INFO scheduler.DAGScheduler: Submitting ShuffleMapStage 0 (PairwiseRDD[3] at reduceByKey at /home/user/ergasia/meros1/q3rdd.py:20), which has no missing parents
21/03/14 05:31:42 INFO memory.MemoryStore: Block broadcast_2 stored as values in memory (estimated size 11.1 KB, free 92.8 MB)
21/03/14 05:31:42 INFO memory.MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 7.2 KB, free 92.8 MB)
21/03/14 05:31:42 INFO storage.BlockManagerInfo: Added broadcast_2_piece0 in memory on master:37795 (size: 7.2 KB, free: 93.2 MB)
21/03/14 05:31:42 INFO spark.SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1161
21/03/14 05:31:42 INFO scheduler.DAGScheduler: Submitting 11 missing tasks from ShuffleMapStage 0 (PairwiseRDD[3] at reduceByKey at /home/user/ergasia/meros1/q3rdd.py:20) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
21/03/14 05:31:42 INFO scheduler.TaskSchedulerImpl: Adding task set 0.0 with 11 tasks
21/03/14 05:31:42 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, 192.168.0.1, executor 0, partition 0, ANY, 7883 bytes)
21/03/14 05:31:42 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1, 192.168.0.2, executor 1, partition 1, ANY, 7883 bytes)
21/03/14 05:31:42 INFO scheduler.TaskSetManager: Starting task 2.0 in stage 0.0 (TID 2, 192.168.0.1, executor 0, partition 2, ANY, 7883 bytes)
21/03/14 05:31:42 INFO scheduler.TaskSetManager: Starting task 3.0 in stage 0.0 (TID 3, 192.168.0.2, executor 1, partition 3, ANY, 7883 bytes)
21/03/14 05:31:43 INFO storage.BlockManagerInfo: Added broadcast_2_piece0 in memory on 192.168.0.2:46470 (size: 7.2 KB, free: 1458.6 MB)
21/03/14 05:31:43 INFO storage.BlockManagerInfo: Added broadcast_2_piece0 in memory on 192.168.0.1:44204 (size: 7.2 KB, free: 1458.6 MB)
21/03/14 05:31:43 INFO storage.BlockManagerInfo: Added broadcast_0_piece0 in memory on 192.168.0.2:46470 (size: 23.1 KB, free: 1458.6 MB)
21/03/14 05:31:43 INFO storage.BlockManagerInfo: Added broadcast_0_piece0 in memory on 192.168.0.1:44204 (size: 23.1 KB, free: 1458.6 MB)
21/03/14 05:32:13 INFO scheduler.TaskSetManager: Starting task 4.0 in stage 0.0 (TID 4, 192.168.0.2, executor 1, partition 4, ANY, 7883 bytes)
21/03/14 05:32:13 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 30449 ms on 192.168.0.2 (executor 1) (1/11)
21/03/14 05:32:13 INFO python.PythonAccumulatorV2: Connected to AccumulatorServer at host: 127.0.0.1 port: 54325
21/03/14 05:32:13 INFO scheduler.TaskSetManager: Starting task 5.0 in stage 0.0 (TID 5, 192.168.0.2, executor 1, partition 5, ANY, 7883 bytes)
21/03/14 05:32:13 INFO scheduler.TaskSetManager: Finished task 3.0 in stage 0.0 (TID 3) in 30722 ms on 192.168.0.2 (executor 1) (2/11)
21/03/14 05:32:17 INFO scheduler.TaskSetManager: Starting task 6.0 in stage 0.0 (TID 6, 192.168.0.1, executor 0, partition 6, ANY, 7883 bytes)
21/03/14 05:32:17 INFO scheduler.TaskSetManager: Finished task 2.0 in stage 0.0 (TID 2) in 34831 ms on 192.168.0.1 (executor 0) (3/11)
21/03/14 05:32:18 INFO scheduler.TaskSetManager: Starting task 7.0 in stage 0.0 (TID 7, 192.168.0.1, executor 0, partition 7, ANY, 7883 bytes)
21/03/14 05:32:18 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 35926 ms on 192.168.0.1 (executor 0) (4/11)
21/03/14 05:32:37 INFO scheduler.TaskSetManager: Starting task 8.0 in stage 0.0 (TID 8, 192.168.0.2, executor 1, partition 8, ANY, 7883 bytes)
21/03/14 05:32:37 INFO scheduler.TaskSetManager: Finished task 5.0 in stage 0.0 (TID 5) in 23915 ms on 192.168.0.2 (executor 1) (5/11)
21/03/14 05:32:37 INFO scheduler.TaskSetManager: Starting task 9.0 in stage 0.0 (TID 9, 192.168.0.2, executor 1, partition 9, ANY, 7883 bytes)
21/03/14 05:32:37 INFO scheduler.TaskSetManager: Finished task 4.0 in stage 0.0 (TID 4) in 24273 ms on 192.168.0.2 (executor 1) (6/11)
21/03/14 05:32:48 INFO scheduler.TaskSetManager: Starting task 10.0 in stage 0.0 (TID 10, 192.168.0.1, executor 0, partition 10, ANY, 7883 bytes)
21/03/14 05:32:48 INFO scheduler.TaskSetManager: Finished task 6.0 in stage 0.0 (TID 6) in 30815 ms on 192.168.0.1 (executor 0) (7/11)
21/03/14 05:32:49 INFO scheduler.TaskSetManager: Finished task 7.0 in stage 0.0 (TID 7) in 30776 ms on 192.168.0.1 (executor 0) (8/11)
21/03/14 05:33:01 INFO scheduler.TaskSetManager: Finished task 8.0 in stage 0.0 (TID 8) in 23778 ms on 192.168.0.2 (executor 1) (9/11)
21/03/14 05:33:01 INFO scheduler.TaskSetManager: Finished task 9.0 in stage 0.0 (TID 9) in 24251 ms on 192.168.0.2 (executor 1) (10/11)
21/03/14 05:33:05 INFO scheduler.TaskSetManager: Finished task 10.0 in stage 0.0 (TID 10) in 16999 ms on 192.168.0.1 (executor 0) (11/11)
21/03/14 05:33:05 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
21/03/14 05:33:05 INFO scheduler.DAGScheduler: ShuffleMapStage 0 (reduceByKey at /home/user/ergasia/meros1/q3rdd.py:20) finished in 82.722 s
21/03/14 05:33:05 INFO scheduler.DAGScheduler: looking for newly runnable stages
21/03/14 05:33:05 INFO scheduler.DAGScheduler: running: Set()
21/03/14 05:33:05 INFO scheduler.DAGScheduler: waiting: Set(ShuffleMapStage 1, ShuffleMapStage 2, ResultStage 3)
21/03/14 05:33:05 INFO scheduler.DAGScheduler: failed: Set()
21/03/14 05:33:05 INFO scheduler.DAGScheduler: Submitting ShuffleMapStage 1 (PairwiseRDD[12] at join at /home/user/ergasia/meros1/q3rdd.py:28), which has no missing parents
21/03/14 05:33:05 INFO memory.MemoryStore: Block broadcast_3 stored as values in memory (estimated size 17.4 KB, free 92.8 MB)
21/03/14 05:33:05 INFO memory.MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 9.1 KB, free 92.7 MB)
21/03/14 05:33:05 INFO storage.BlockManagerInfo: Added broadcast_3_piece0 in memory on master:37795 (size: 9.1 KB, free: 93.2 MB)
21/03/14 05:33:05 INFO spark.SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1161
21/03/14 05:33:05 INFO scheduler.DAGScheduler: Submitting 13 missing tasks from ShuffleMapStage 1 (PairwiseRDD[12] at join at /home/user/ergasia/meros1/q3rdd.py:28) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
21/03/14 05:33:05 INFO scheduler.TaskSchedulerImpl: Adding task set 1.0 with 13 tasks
21/03/14 05:33:05 INFO scheduler.TaskSetManager: Starting task 2.0 in stage 1.0 (TID 11, 192.168.0.1, executor 0, partition 2, NODE_LOCAL, 7764 bytes)
21/03/14 05:33:05 INFO scheduler.TaskSetManager: Starting task 3.0 in stage 1.0 (TID 12, 192.168.0.2, executor 1, partition 3, NODE_LOCAL, 7764 bytes)
21/03/14 05:33:05 INFO scheduler.TaskSetManager: Starting task 4.0 in stage 1.0 (TID 13, 192.168.0.1, executor 0, partition 4, NODE_LOCAL, 7764 bytes)
21/03/14 05:33:05 INFO scheduler.TaskSetManager: Starting task 5.0 in stage 1.0 (TID 14, 192.168.0.2, executor 1, partition 5, NODE_LOCAL, 7764 bytes)
21/03/14 05:33:05 INFO storage.BlockManagerInfo: Added broadcast_3_piece0 in memory on 192.168.0.2:46470 (size: 9.1 KB, free: 1458.6 MB)
21/03/14 05:33:05 INFO storage.BlockManagerInfo: Added broadcast_3_piece0 in memory on 192.168.0.1:44204 (size: 9.1 KB, free: 1458.6 MB)
21/03/14 05:33:05 INFO spark.MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 2 to 192.168.0.2:43928
21/03/14 05:33:05 INFO spark.MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 2 to 192.168.0.1:58384
21/03/14 05:33:05 INFO scheduler.TaskSetManager: Starting task 6.0 in stage 1.0 (TID 15, 192.168.0.2, executor 1, partition 6, NODE_LOCAL, 7764 bytes)
21/03/14 05:33:05 INFO scheduler.TaskSetManager: Starting task 7.0 in stage 1.0 (TID 16, 192.168.0.1, executor 0, partition 7, NODE_LOCAL, 7764 bytes)
21/03/14 05:33:05 INFO scheduler.TaskSetManager: Finished task 5.0 in stage 1.0 (TID 14) in 407 ms on 192.168.0.2 (executor 1) (1/13)
21/03/14 05:33:05 INFO scheduler.TaskSetManager: Finished task 2.0 in stage 1.0 (TID 11) in 415 ms on 192.168.0.1 (executor 0) (2/13)
21/03/14 05:33:05 INFO scheduler.TaskSetManager: Starting task 8.0 in stage 1.0 (TID 17, 192.168.0.2, executor 1, partition 8, NODE_LOCAL, 7764 bytes)
21/03/14 05:33:05 INFO scheduler.TaskSetManager: Finished task 3.0 in stage 1.0 (TID 12) in 447 ms on 192.168.0.2 (executor 1) (3/13)
21/03/14 05:33:05 INFO scheduler.TaskSetManager: Starting task 9.0 in stage 1.0 (TID 18, 192.168.0.1, executor 0, partition 9, NODE_LOCAL, 7764 bytes)
21/03/14 05:33:05 INFO scheduler.TaskSetManager: Finished task 4.0 in stage 1.0 (TID 13) in 465 ms on 192.168.0.1 (executor 0) (4/13)
21/03/14 05:33:06 INFO scheduler.TaskSetManager: Starting task 10.0 in stage 1.0 (TID 19, 192.168.0.2, executor 1, partition 10, NODE_LOCAL, 7764 bytes)
21/03/14 05:33:06 INFO scheduler.TaskSetManager: Finished task 6.0 in stage 1.0 (TID 15) in 248 ms on 192.168.0.2 (executor 1) (5/13)
21/03/14 05:33:06 INFO scheduler.TaskSetManager: Starting task 11.0 in stage 1.0 (TID 20, 192.168.0.1, executor 0, partition 11, NODE_LOCAL, 7764 bytes)
21/03/14 05:33:06 INFO scheduler.TaskSetManager: Finished task 9.0 in stage 1.0 (TID 18) in 242 ms on 192.168.0.1 (executor 0) (6/13)
21/03/14 05:33:06 INFO scheduler.TaskSetManager: Starting task 12.0 in stage 1.0 (TID 21, 192.168.0.1, executor 0, partition 12, NODE_LOCAL, 7764 bytes)
21/03/14 05:33:06 INFO scheduler.TaskSetManager: Finished task 7.0 in stage 1.0 (TID 16) in 338 ms on 192.168.0.1 (executor 0) (7/13)
21/03/14 05:33:06 INFO scheduler.TaskSetManager: Finished task 8.0 in stage 1.0 (TID 17) in 351 ms on 192.168.0.2 (executor 1) (8/13)
21/03/14 05:33:06 INFO scheduler.TaskSetManager: Finished task 10.0 in stage 1.0 (TID 19) in 220 ms on 192.168.0.2 (executor 1) (9/13)
21/03/14 05:33:06 INFO scheduler.TaskSetManager: Finished task 12.0 in stage 1.0 (TID 21) in 309 ms on 192.168.0.1 (executor 0) (10/13)
21/03/14 05:33:06 INFO scheduler.TaskSetManager: Finished task 11.0 in stage 1.0 (TID 20) in 363 ms on 192.168.0.1 (executor 0) (11/13)
21/03/14 05:33:09 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 1.0 (TID 22, 192.168.0.1, executor 0, partition 0, ANY, 7997 bytes)
21/03/14 05:33:09 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 1.0 (TID 23, 192.168.0.2, executor 1, partition 1, ANY, 7997 bytes)
21/03/14 05:33:09 INFO storage.BlockManagerInfo: Added broadcast_1_piece0 in memory on 192.168.0.1:44204 (size: 23.1 KB, free: 1458.5 MB)
21/03/14 05:33:09 INFO storage.BlockManagerInfo: Added broadcast_1_piece0 in memory on 192.168.0.2:46470 (size: 23.1 KB, free: 1458.5 MB)
21/03/14 05:33:10 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 1.0 (TID 23) in 629 ms on 192.168.0.2 (executor 1) (12/13)
21/03/14 05:33:10 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 1.0 (TID 22) in 819 ms on 192.168.0.1 (executor 0) (13/13)
21/03/14 05:33:10 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
21/03/14 05:33:10 INFO scheduler.DAGScheduler: ShuffleMapStage 1 (join at /home/user/ergasia/meros1/q3rdd.py:28) finished in 5.041 s
21/03/14 05:33:10 INFO scheduler.DAGScheduler: looking for newly runnable stages
21/03/14 05:33:10 INFO scheduler.DAGScheduler: running: Set()
21/03/14 05:33:10 INFO scheduler.DAGScheduler: waiting: Set(ShuffleMapStage 2, ResultStage 3)
21/03/14 05:33:10 INFO scheduler.DAGScheduler: failed: Set()
21/03/14 05:33:10 INFO scheduler.DAGScheduler: Submitting ShuffleMapStage 2 (PairwiseRDD[16] at reduceByKey at /home/user/ergasia/meros1/q3rdd.py:30), which has no missing parents
21/03/14 05:33:10 INFO memory.MemoryStore: Block broadcast_4 stored as values in memory (estimated size 13.2 KB, free 92.7 MB)
21/03/14 05:33:10 INFO memory.MemoryStore: Block broadcast_4_piece0 stored as bytes in memory (estimated size 8.5 KB, free 92.7 MB)
21/03/14 05:33:10 INFO storage.BlockManagerInfo: Added broadcast_4_piece0 in memory on master:37795 (size: 8.5 KB, free: 93.2 MB)
21/03/14 05:33:10 INFO spark.SparkContext: Created broadcast 4 from broadcast at DAGScheduler.scala:1161
21/03/14 05:33:10 INFO scheduler.DAGScheduler: Submitting 13 missing tasks from ShuffleMapStage 2 (PairwiseRDD[16] at reduceByKey at /home/user/ergasia/meros1/q3rdd.py:30) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
21/03/14 05:33:10 INFO scheduler.TaskSchedulerImpl: Adding task set 2.0 with 13 tasks
21/03/14 05:33:10 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 2.0 (TID 24, 192.168.0.1, executor 0, partition 0, NODE_LOCAL, 7655 bytes)
21/03/14 05:33:10 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 2.0 (TID 25, 192.168.0.2, executor 1, partition 1, NODE_LOCAL, 7655 bytes)
21/03/14 05:33:10 INFO scheduler.TaskSetManager: Starting task 2.0 in stage 2.0 (TID 26, 192.168.0.1, executor 0, partition 2, NODE_LOCAL, 7655 bytes)
21/03/14 05:33:10 INFO scheduler.TaskSetManager: Starting task 3.0 in stage 2.0 (TID 27, 192.168.0.2, executor 1, partition 3, NODE_LOCAL, 7655 bytes)
21/03/14 05:33:10 INFO storage.BlockManagerInfo: Added broadcast_4_piece0 in memory on 192.168.0.2:46470 (size: 8.5 KB, free: 1458.5 MB)
21/03/14 05:33:10 INFO storage.BlockManagerInfo: Added broadcast_4_piece0 in memory on 192.168.0.1:44204 (size: 8.5 KB, free: 1458.5 MB)
21/03/14 05:33:10 INFO spark.MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 1 to 192.168.0.2:43928
21/03/14 05:33:10 INFO spark.MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 1 to 192.168.0.1:58384
21/03/14 05:33:10 INFO scheduler.TaskSetManager: Starting task 4.0 in stage 2.0 (TID 28, 192.168.0.1, executor 0, partition 4, NODE_LOCAL, 7655 bytes)
21/03/14 05:33:10 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 2.0 (TID 24) in 141 ms on 192.168.0.1 (executor 0) (1/13)
21/03/14 05:33:10 INFO scheduler.TaskSetManager: Starting task 5.0 in stage 2.0 (TID 29, 192.168.0.2, executor 1, partition 5, NODE_LOCAL, 7655 bytes)
21/03/14 05:33:10 INFO scheduler.TaskSetManager: Finished task 3.0 in stage 2.0 (TID 27) in 154 ms on 192.168.0.2 (executor 1) (2/13)
21/03/14 05:33:10 INFO scheduler.TaskSetManager: Starting task 6.0 in stage 2.0 (TID 30, 192.168.0.2, executor 1, partition 6, NODE_LOCAL, 7655 bytes)
21/03/14 05:33:10 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 2.0 (TID 25) in 163 ms on 192.168.0.2 (executor 1) (3/13)
21/03/14 05:33:10 INFO scheduler.TaskSetManager: Starting task 7.0 in stage 2.0 (TID 31, 192.168.0.1, executor 0, partition 7, NODE_LOCAL, 7655 bytes)
21/03/14 05:33:10 INFO scheduler.TaskSetManager: Finished task 2.0 in stage 2.0 (TID 26) in 230 ms on 192.168.0.1 (executor 0) (4/13)
21/03/14 05:33:10 INFO scheduler.TaskSetManager: Starting task 8.0 in stage 2.0 (TID 32, 192.168.0.2, executor 1, partition 8, NODE_LOCAL, 7655 bytes)
21/03/14 05:33:10 INFO scheduler.TaskSetManager: Finished task 5.0 in stage 2.0 (TID 29) in 128 ms on 192.168.0.2 (executor 1) (5/13)
21/03/14 05:33:10 INFO scheduler.TaskSetManager: Starting task 9.0 in stage 2.0 (TID 33, 192.168.0.2, executor 1, partition 9, NODE_LOCAL, 7655 bytes)
21/03/14 05:33:10 INFO scheduler.TaskSetManager: Finished task 6.0 in stage 2.0 (TID 30) in 149 ms on 192.168.0.2 (executor 1) (6/13)
21/03/14 05:33:10 INFO scheduler.TaskSetManager: Starting task 10.0 in stage 2.0 (TID 34, 192.168.0.1, executor 0, partition 10, NODE_LOCAL, 7655 bytes)
21/03/14 05:33:10 INFO scheduler.TaskSetManager: Finished task 4.0 in stage 2.0 (TID 28) in 195 ms on 192.168.0.1 (executor 0) (7/13)
21/03/14 05:33:10 INFO scheduler.TaskSetManager: Starting task 11.0 in stage 2.0 (TID 35, 192.168.0.1, executor 0, partition 11, NODE_LOCAL, 7655 bytes)
21/03/14 05:33:10 INFO scheduler.TaskSetManager: Finished task 7.0 in stage 2.0 (TID 31) in 170 ms on 192.168.0.1 (executor 0) (8/13)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Starting task 12.0 in stage 2.0 (TID 36, 192.168.0.2, executor 1, partition 12, NODE_LOCAL, 7655 bytes)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Finished task 8.0 in stage 2.0 (TID 32) in 152 ms on 192.168.0.2 (executor 1) (9/13)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Finished task 9.0 in stage 2.0 (TID 33) in 169 ms on 192.168.0.2 (executor 1) (10/13)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Finished task 12.0 in stage 2.0 (TID 36) in 107 ms on 192.168.0.2 (executor 1) (11/13)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Finished task 11.0 in stage 2.0 (TID 35) in 144 ms on 192.168.0.1 (executor 0) (12/13)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Finished task 10.0 in stage 2.0 (TID 34) in 227 ms on 192.168.0.1 (executor 0) (13/13)
21/03/14 05:33:11 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 2.0, whose tasks have all completed, from pool 
21/03/14 05:33:11 INFO scheduler.DAGScheduler: ShuffleMapStage 2 (reduceByKey at /home/user/ergasia/meros1/q3rdd.py:30) finished in 0.574 s
21/03/14 05:33:11 INFO scheduler.DAGScheduler: looking for newly runnable stages
21/03/14 05:33:11 INFO scheduler.DAGScheduler: running: Set()
21/03/14 05:33:11 INFO scheduler.DAGScheduler: waiting: Set(ResultStage 3)
21/03/14 05:33:11 INFO scheduler.DAGScheduler: failed: Set()
21/03/14 05:33:11 INFO scheduler.DAGScheduler: Submitting ResultStage 3 (PythonRDD[19] at collect at /home/user/ergasia/meros1/q3rdd.py:31), which has no missing parents
21/03/14 05:33:11 INFO memory.MemoryStore: Block broadcast_5 stored as values in memory (estimated size 8.0 KB, free 92.7 MB)
21/03/14 05:33:11 INFO memory.MemoryStore: Block broadcast_5_piece0 stored as bytes in memory (estimated size 5.1 KB, free 92.7 MB)
21/03/14 05:33:11 INFO storage.BlockManagerInfo: Added broadcast_5_piece0 in memory on master:37795 (size: 5.1 KB, free: 93.2 MB)
21/03/14 05:33:11 INFO spark.SparkContext: Created broadcast 5 from broadcast at DAGScheduler.scala:1161
21/03/14 05:33:11 INFO scheduler.DAGScheduler: Submitting 13 missing tasks from ResultStage 3 (PythonRDD[19] at collect at /home/user/ergasia/meros1/q3rdd.py:31) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
21/03/14 05:33:11 INFO scheduler.TaskSchedulerImpl: Adding task set 3.0 with 13 tasks
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 3.0 (TID 37, 192.168.0.1, executor 0, partition 0, NODE_LOCAL, 7666 bytes)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 3.0 (TID 38, 192.168.0.2, executor 1, partition 1, NODE_LOCAL, 7666 bytes)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Starting task 2.0 in stage 3.0 (TID 39, 192.168.0.1, executor 0, partition 2, NODE_LOCAL, 7666 bytes)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Starting task 3.0 in stage 3.0 (TID 40, 192.168.0.2, executor 1, partition 3, NODE_LOCAL, 7666 bytes)
21/03/14 05:33:11 INFO storage.BlockManagerInfo: Added broadcast_5_piece0 in memory on 192.168.0.1:44204 (size: 5.1 KB, free: 1458.5 MB)
21/03/14 05:33:11 INFO spark.MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 0 to 192.168.0.1:58384
21/03/14 05:33:11 INFO storage.BlockManagerInfo: Added broadcast_5_piece0 in memory on 192.168.0.2:46470 (size: 5.1 KB, free: 1458.5 MB)
21/03/14 05:33:11 INFO spark.MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 0 to 192.168.0.2:43928
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Starting task 5.0 in stage 3.0 (TID 41, 192.168.0.1, executor 0, partition 5, NODE_LOCAL, 7666 bytes)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Finished task 2.0 in stage 3.0 (TID 39) in 87 ms on 192.168.0.1 (executor 0) (1/13)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Starting task 6.0 in stage 3.0 (TID 42, 192.168.0.1, executor 0, partition 6, NODE_LOCAL, 7666 bytes)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 3.0 (TID 37) in 114 ms on 192.168.0.1 (executor 0) (2/13)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Starting task 7.0 in stage 3.0 (TID 43, 192.168.0.2, executor 1, partition 7, NODE_LOCAL, 7666 bytes)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Starting task 8.0 in stage 3.0 (TID 44, 192.168.0.2, executor 1, partition 8, NODE_LOCAL, 7666 bytes)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Finished task 3.0 in stage 3.0 (TID 40) in 121 ms on 192.168.0.2 (executor 1) (3/13)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 3.0 (TID 38) in 123 ms on 192.168.0.2 (executor 1) (4/13)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Starting task 9.0 in stage 3.0 (TID 45, 192.168.0.2, executor 1, partition 9, NODE_LOCAL, 7666 bytes)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Finished task 8.0 in stage 3.0 (TID 44) in 36 ms on 192.168.0.2 (executor 1) (5/13)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Starting task 11.0 in stage 3.0 (TID 46, 192.168.0.2, executor 1, partition 11, NODE_LOCAL, 7666 bytes)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Finished task 7.0 in stage 3.0 (TID 43) in 74 ms on 192.168.0.2 (executor 1) (6/13)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Starting task 4.0 in stage 3.0 (TID 47, 192.168.0.1, executor 0, partition 4, PROCESS_LOCAL, 7666 bytes)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Starting task 10.0 in stage 3.0 (TID 48, 192.168.0.1, executor 0, partition 10, PROCESS_LOCAL, 7666 bytes)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Starting task 12.0 in stage 3.0 (TID 49, 192.168.0.2, executor 1, partition 12, PROCESS_LOCAL, 7666 bytes)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Finished task 6.0 in stage 3.0 (TID 42) in 107 ms on 192.168.0.1 (executor 0) (7/13)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Finished task 9.0 in stage 3.0 (TID 45) in 63 ms on 192.168.0.2 (executor 1) (8/13)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Finished task 5.0 in stage 3.0 (TID 41) in 133 ms on 192.168.0.1 (executor 0) (9/13)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Finished task 11.0 in stage 3.0 (TID 46) in 57 ms on 192.168.0.2 (executor 1) (10/13)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Finished task 12.0 in stage 3.0 (TID 49) in 55 ms on 192.168.0.2 (executor 1) (11/13)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Finished task 4.0 in stage 3.0 (TID 47) in 75 ms on 192.168.0.1 (executor 0) (12/13)
21/03/14 05:33:11 INFO scheduler.TaskSetManager: Finished task 10.0 in stage 3.0 (TID 48) in 75 ms on 192.168.0.1 (executor 0) (13/13)
21/03/14 05:33:11 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 3.0, whose tasks have all completed, from pool 
21/03/14 05:33:11 INFO scheduler.DAGScheduler: ResultStage 3 (collect at /home/user/ergasia/meros1/q3rdd.py:31) finished in 0.312 s
21/03/14 05:33:11 INFO scheduler.DAGScheduler: Job 0 finished: collect at /home/user/ergasia/meros1/q3rdd.py:31, took 88.773857 s
('Romance', (3.156277701839363, 1206))
('Western', (3.2140607181871017, 185))
('Crime', (3.1625839378882312, 907))
('Drama', (3.1391216242914504, 3724))
('Action', (3.1585329294510713, 1175))
('Comedy', (3.1370494677399536, 2113))
('Horror', (3.134020262392476, 723))
('Music', (3.189047782134767, 248))
('TV Movie', (3.148118069220997, 87))
('Science Fiction', (3.157163683913988, 593))
('Animation', (3.175885456020015, 223))
('Adventure', (3.168709062525581, 707))
('Thriller', (3.148281748166091, 1427))
('History', (3.1187910458495414, 306))
('Family', (3.1569054919202837, 386))
('Mystery', (3.148769392655548, 475))
('Fantasy', (3.1429135219007014, 463))
('Foreign', (3.1139858344079134, 271))
('War', (3.1146633826061416, 231))
('Documentary', (3.0742356093239853, 503))
Q3_RDD execution time: 94.70336699485779 seconds
21/03/14 05:33:11 INFO spark.SparkContext: Invoking stop() from shutdown hook
21/03/14 05:33:11 INFO server.AbstractConnector: Stopped Spark@235c0abc{HTTP/1.1,[http/1.1]}{0.0.0.0:4040}
21/03/14 05:33:11 INFO ui.SparkUI: Stopped Spark web UI at http://master:4040
21/03/14 05:33:11 INFO cluster.StandaloneSchedulerBackend: Shutting down all executors
21/03/14 05:33:11 INFO cluster.CoarseGrainedSchedulerBackend$DriverEndpoint: Asking each executor to shut down
21/03/14 05:33:11 INFO spark.MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
21/03/14 05:33:11 INFO memory.MemoryStore: MemoryStore cleared
21/03/14 05:33:11 INFO storage.BlockManager: BlockManager stopped
21/03/14 05:33:11 INFO storage.BlockManagerMaster: BlockManagerMaster stopped
21/03/14 05:33:11 INFO scheduler.OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
21/03/14 05:33:11 INFO spark.SparkContext: Successfully stopped SparkContext
21/03/14 05:33:11 INFO util.ShutdownHookManager: Shutdown hook called
21/03/14 05:33:11 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-bd4c34f3-c7d4-4cd5-b6f9-446b2ea4278d/pyspark-c4e37b9e-c9e0-4841-b11d-b29dfc0b9f65
21/03/14 05:33:11 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-bd4c34f3-c7d4-4cd5-b6f9-446b2ea4278d
21/03/14 05:33:11 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-e730df12-6d9c-4e14-aec6-4855d78bc15b
]0;user@master: ~/ergasia/meros1user@master:~/ergasia/meros1$ exit
exit

Script done on Sun 14 Mar 2021 05:33:20 AM EET
