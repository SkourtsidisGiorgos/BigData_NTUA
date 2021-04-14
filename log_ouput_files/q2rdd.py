Script started on Sun 14 Mar 2021 05:29:58 AM EET
]0;user@master: ~/ergasia/meros1user@master:~/ergasia/meros1$ exitstoppark-submit q1rdd.py P[1P[1@2
21/03/14 05:30:05 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
21/03/14 05:30:06 INFO spark.SparkContext: Running Spark version 2.4.4
21/03/14 05:30:06 INFO spark.SparkContext: Submitted application: q2_rdd
21/03/14 05:30:06 INFO spark.SecurityManager: Changing view acls to: user
21/03/14 05:30:06 INFO spark.SecurityManager: Changing modify acls to: user
21/03/14 05:30:06 INFO spark.SecurityManager: Changing view acls groups to: 
21/03/14 05:30:06 INFO spark.SecurityManager: Changing modify acls groups to: 
21/03/14 05:30:06 INFO spark.SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(user); groups with view permissions: Set(); users  with modify permissions: Set(user); groups with modify permissions: Set()
21/03/14 05:30:06 INFO util.Utils: Successfully started service 'sparkDriver' on port 43566.
21/03/14 05:30:06 INFO spark.SparkEnv: Registering MapOutputTracker
21/03/14 05:30:06 INFO spark.SparkEnv: Registering BlockManagerMaster
21/03/14 05:30:06 INFO storage.BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
21/03/14 05:30:06 INFO storage.BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
21/03/14 05:30:06 INFO storage.DiskBlockManager: Created local directory at /tmp/blockmgr-5dab5d8d-2592-4d96-8108-131e84f07e98
21/03/14 05:30:06 INFO memory.MemoryStore: MemoryStore started with capacity 93.3 MB
21/03/14 05:30:06 INFO spark.SparkEnv: Registering OutputCommitCoordinator
21/03/14 05:30:06 INFO util.log: Logging initialized @2536ms
21/03/14 05:30:06 INFO server.Server: jetty-9.3.z-SNAPSHOT, build timestamp: unknown, git hash: unknown
21/03/14 05:30:06 INFO server.Server: Started @2616ms
21/03/14 05:30:07 INFO server.AbstractConnector: Started ServerConnector@235c0abc{HTTP/1.1,[http/1.1]}{0.0.0.0:4040}
21/03/14 05:30:07 INFO util.Utils: Successfully started service 'SparkUI' on port 4040.
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@2b81ac15{/jobs,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@6ec5950c{/jobs/json,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@657a544c{/jobs/job,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@572183e2{/jobs/job/json,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@150e11ff{/stages,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@2261ff26{/stages/json,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@5329ea27{/stages/stage,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@409d7e70{/stages/stage/json,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@5cf9cc96{/stages/pool,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@36b627ef{/stages/pool/json,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@1a13fc51{/storage,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@31567f9d{/storage/json,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@23d9885d{/storage/rdd,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@24574a96{/storage/rdd/json,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@21a3db39{/environment,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@230bf207{/environment/json,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@4cff1d5b{/executors,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@52e2fda8{/executors/json,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@6d278ede{/executors/threadDump,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@1140d2b7{/executors/threadDump/json,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@25611001{/static,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@5afbe2f3{/,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@6aa6f4c4{/api,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@3487451{/jobs/job/kill,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@4f4d42e9{/stages/stage/kill,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO ui.SparkUI: Bound SparkUI to 0.0.0.0, and started at http://master:4040
21/03/14 05:30:07 INFO client.StandaloneAppClient$ClientEndpoint: Connecting to master spark://master:7077...
21/03/14 05:30:07 INFO client.TransportClientFactory: Successfully created connection to master/192.168.0.1:7077 after 41 ms (0 ms spent in bootstraps)
21/03/14 05:30:07 INFO cluster.StandaloneSchedulerBackend: Connected to Spark cluster with app ID app-20210314053007-0098
21/03/14 05:30:07 INFO client.StandaloneAppClient$ClientEndpoint: Executor added: app-20210314053007-0098/0 on worker-20210313132157-192.168.0.1-43826 (192.168.0.1:43826) with 2 core(s)
21/03/14 05:30:07 INFO cluster.StandaloneSchedulerBackend: Granted executor ID app-20210314053007-0098/0 on hostPort 192.168.0.1:43826 with 2 core(s), 3.0 GB RAM
21/03/14 05:30:07 INFO client.StandaloneAppClient$ClientEndpoint: Executor added: app-20210314053007-0098/1 on worker-20210313132158-192.168.0.2-46469 (192.168.0.2:46469) with 2 core(s)
21/03/14 05:30:07 INFO cluster.StandaloneSchedulerBackend: Granted executor ID app-20210314053007-0098/1 on hostPort 192.168.0.2:46469 with 2 core(s), 3.0 GB RAM
21/03/14 05:30:07 INFO client.StandaloneAppClient$ClientEndpoint: Executor updated: app-20210314053007-0098/1 is now RUNNING
21/03/14 05:30:07 INFO util.Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 41718.
21/03/14 05:30:07 INFO client.StandaloneAppClient$ClientEndpoint: Executor updated: app-20210314053007-0098/0 is now RUNNING
21/03/14 05:30:07 INFO netty.NettyBlockTransferService: Server created on master:41718
21/03/14 05:30:07 INFO storage.BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
21/03/14 05:30:07 INFO storage.BlockManagerMaster: Registering BlockManager BlockManagerId(driver, master, 41718, None)
21/03/14 05:30:07 INFO storage.BlockManagerMasterEndpoint: Registering block manager master:41718 with 93.3 MB RAM, BlockManagerId(driver, master, 41718, None)
21/03/14 05:30:07 INFO storage.BlockManagerMaster: Registered BlockManager BlockManagerId(driver, master, 41718, None)
21/03/14 05:30:07 INFO storage.BlockManager: Initialized BlockManager: BlockManagerId(driver, master, 41718, None)
21/03/14 05:30:07 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@58410de5{/metrics/json,null,AVAILABLE,@Spark}
21/03/14 05:30:07 INFO cluster.StandaloneSchedulerBackend: SchedulerBackend is ready for scheduling beginning after reached minRegisteredResourcesRatio: 0.0
21/03/14 05:30:08 INFO internal.SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir ('file:/home/user/ergasia/meros1/spark-warehouse/').
21/03/14 05:30:08 INFO internal.SharedState: Warehouse path is 'file:/home/user/ergasia/meros1/spark-warehouse/'.
21/03/14 05:30:08 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@431de2a2{/SQL,null,AVAILABLE,@Spark}
21/03/14 05:30:08 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@4b30cdf4{/SQL/json,null,AVAILABLE,@Spark}
21/03/14 05:30:08 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@7cbbc0d3{/SQL/execution,null,AVAILABLE,@Spark}
21/03/14 05:30:08 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@50500257{/SQL/execution/json,null,AVAILABLE,@Spark}
21/03/14 05:30:08 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@28dd0fbd{/static/sql,null,AVAILABLE,@Spark}
21/03/14 05:30:09 INFO state.StateStoreCoordinatorRef: Registered StateStoreCoordinator endpoint
21/03/14 05:30:09 INFO cluster.CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (192.168.0.2:45410) with ID 1
21/03/14 05:30:09 INFO storage.BlockManagerMasterEndpoint: Registering block manager 192.168.0.2:42175 with 1458.6 MB RAM, BlockManagerId(1, 192.168.0.2, 42175, None)
21/03/14 05:30:10 INFO memory.MemoryStore: Block broadcast_0 stored as values in memory (estimated size 239.4 KB, free 93.1 MB)
21/03/14 05:30:10 INFO memory.MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 23.1 KB, free 93.0 MB)
21/03/14 05:30:10 INFO storage.BlockManagerInfo: Added broadcast_0_piece0 in memory on master:41718 (size: 23.1 KB, free: 93.3 MB)
21/03/14 05:30:10 INFO spark.SparkContext: Created broadcast 0 from textFile at NativeMethodAccessorImpl.java:0
21/03/14 05:30:11 INFO mapred.FileInputFormat: Total input paths to process : 1
21/03/14 05:30:11 INFO cluster.CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (192.168.0.1:34474) with ID 0
21/03/14 05:30:11 INFO spark.SparkContext: Starting job: collect at /home/user/ergasia/meros1/q2rdd.py:27
21/03/14 05:30:11 INFO scheduler.DAGScheduler: Registering RDD 3 (reduceByKey at /home/user/ergasia/meros1/q2rdd.py:26)
21/03/14 05:30:11 INFO scheduler.DAGScheduler: Got job 0 (collect at /home/user/ergasia/meros1/q2rdd.py:27) with 11 output partitions
21/03/14 05:30:11 INFO scheduler.DAGScheduler: Final stage: ResultStage 1 (collect at /home/user/ergasia/meros1/q2rdd.py:27)
21/03/14 05:30:11 INFO scheduler.DAGScheduler: Parents of final stage: List(ShuffleMapStage 0)
21/03/14 05:30:11 INFO scheduler.DAGScheduler: Missing parents: List(ShuffleMapStage 0)
21/03/14 05:30:11 INFO scheduler.DAGScheduler: Submitting ShuffleMapStage 0 (PairwiseRDD[3] at reduceByKey at /home/user/ergasia/meros1/q2rdd.py:26), which has no missing parents
21/03/14 05:30:11 INFO storage.BlockManagerMasterEndpoint: Registering block manager 192.168.0.1:37240 with 1458.6 MB RAM, BlockManagerId(0, 192.168.0.1, 37240, None)
21/03/14 05:30:11 INFO memory.MemoryStore: Block broadcast_1 stored as values in memory (estimated size 11.4 KB, free 93.0 MB)
21/03/14 05:30:11 INFO memory.MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 7.4 KB, free 93.0 MB)
21/03/14 05:30:11 INFO storage.BlockManagerInfo: Added broadcast_1_piece0 in memory on master:41718 (size: 7.4 KB, free: 93.3 MB)
21/03/14 05:30:11 INFO spark.SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1161
21/03/14 05:30:11 INFO scheduler.DAGScheduler: Submitting 11 missing tasks from ShuffleMapStage 0 (PairwiseRDD[3] at reduceByKey at /home/user/ergasia/meros1/q2rdd.py:26) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
21/03/14 05:30:11 INFO scheduler.TaskSchedulerImpl: Adding task set 0.0 with 11 tasks
21/03/14 05:30:11 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, 192.168.0.2, executor 1, partition 0, ANY, 7883 bytes)
21/03/14 05:30:11 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1, 192.168.0.1, executor 0, partition 1, ANY, 7883 bytes)
21/03/14 05:30:11 INFO scheduler.TaskSetManager: Starting task 2.0 in stage 0.0 (TID 2, 192.168.0.2, executor 1, partition 2, ANY, 7883 bytes)
21/03/14 05:30:11 INFO scheduler.TaskSetManager: Starting task 3.0 in stage 0.0 (TID 3, 192.168.0.1, executor 0, partition 3, ANY, 7883 bytes)
21/03/14 05:30:12 INFO storage.BlockManagerInfo: Added broadcast_1_piece0 in memory on 192.168.0.2:42175 (size: 7.4 KB, free: 1458.6 MB)
21/03/14 05:30:12 INFO storage.BlockManagerInfo: Added broadcast_1_piece0 in memory on 192.168.0.1:37240 (size: 7.4 KB, free: 1458.6 MB)
21/03/14 05:30:12 INFO storage.BlockManagerInfo: Added broadcast_0_piece0 in memory on 192.168.0.2:42175 (size: 23.1 KB, free: 1458.6 MB)
21/03/14 05:30:12 INFO storage.BlockManagerInfo: Added broadcast_0_piece0 in memory on 192.168.0.1:37240 (size: 23.1 KB, free: 1458.6 MB)
21/03/14 05:30:32 INFO scheduler.TaskSetManager: Starting task 4.0 in stage 0.0 (TID 4, 192.168.0.1, executor 0, partition 4, ANY, 7883 bytes)
21/03/14 05:30:32 INFO scheduler.TaskSetManager: Finished task 3.0 in stage 0.0 (TID 3) in 20821 ms on 192.168.0.1 (executor 0) (1/11)
21/03/14 05:30:32 INFO python.PythonAccumulatorV2: Connected to AccumulatorServer at host: 127.0.0.1 port: 55055
21/03/14 05:30:34 INFO scheduler.TaskSetManager: Starting task 5.0 in stage 0.0 (TID 5, 192.168.0.2, executor 1, partition 5, ANY, 7883 bytes)
21/03/14 05:30:34 INFO scheduler.TaskSetManager: Starting task 6.0 in stage 0.0 (TID 6, 192.168.0.2, executor 1, partition 6, ANY, 7883 bytes)
21/03/14 05:30:34 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 22459 ms on 192.168.0.2 (executor 1) (2/11)
21/03/14 05:30:34 INFO scheduler.TaskSetManager: Starting task 7.0 in stage 0.0 (TID 7, 192.168.0.1, executor 0, partition 7, ANY, 7883 bytes)
21/03/14 05:30:34 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 22441 ms on 192.168.0.1 (executor 0) (3/11)
21/03/14 05:30:34 INFO scheduler.TaskSetManager: Finished task 2.0 in stage 0.0 (TID 2) in 22448 ms on 192.168.0.2 (executor 1) (4/11)
21/03/14 05:30:51 INFO scheduler.TaskSetManager: Starting task 8.0 in stage 0.0 (TID 8, 192.168.0.2, executor 1, partition 8, ANY, 7883 bytes)
21/03/14 05:30:51 INFO scheduler.TaskSetManager: Finished task 5.0 in stage 0.0 (TID 5) in 17083 ms on 192.168.0.2 (executor 1) (5/11)
21/03/14 05:30:51 INFO scheduler.TaskSetManager: Starting task 9.0 in stage 0.0 (TID 9, 192.168.0.1, executor 0, partition 9, ANY, 7883 bytes)
21/03/14 05:30:51 INFO scheduler.TaskSetManager: Finished task 4.0 in stage 0.0 (TID 4) in 18749 ms on 192.168.0.1 (executor 0) (6/11)
21/03/14 05:30:51 INFO scheduler.TaskSetManager: Starting task 10.0 in stage 0.0 (TID 10, 192.168.0.2, executor 1, partition 10, ANY, 7883 bytes)
21/03/14 05:30:51 INFO scheduler.TaskSetManager: Finished task 6.0 in stage 0.0 (TID 6) in 17353 ms on 192.168.0.2 (executor 1) (7/11)
21/03/14 05:30:51 INFO scheduler.TaskSetManager: Finished task 7.0 in stage 0.0 (TID 7) in 17651 ms on 192.168.0.1 (executor 0) (8/11)
21/03/14 05:31:01 INFO scheduler.TaskSetManager: Finished task 10.0 in stage 0.0 (TID 10) in 9638 ms on 192.168.0.2 (executor 1) (9/11)
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Finished task 9.0 in stage 0.0 (TID 9) in 16624 ms on 192.168.0.1 (executor 0) (10/11)
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Finished task 8.0 in stage 0.0 (TID 8) in 16741 ms on 192.168.0.2 (executor 1) (11/11)
21/03/14 05:31:08 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
21/03/14 05:31:08 INFO scheduler.DAGScheduler: ShuffleMapStage 0 (reduceByKey at /home/user/ergasia/meros1/q2rdd.py:26) finished in 56.327 s
21/03/14 05:31:08 INFO scheduler.DAGScheduler: looking for newly runnable stages
21/03/14 05:31:08 INFO scheduler.DAGScheduler: running: Set()
21/03/14 05:31:08 INFO scheduler.DAGScheduler: waiting: Set(ResultStage 1)
21/03/14 05:31:08 INFO scheduler.DAGScheduler: failed: Set()
21/03/14 05:31:08 INFO scheduler.DAGScheduler: Submitting ResultStage 1 (PythonRDD[6] at collect at /home/user/ergasia/meros1/q2rdd.py:27), which has no missing parents
21/03/14 05:31:08 INFO memory.MemoryStore: Block broadcast_2 stored as values in memory (estimated size 8.0 KB, free 93.0 MB)
21/03/14 05:31:08 INFO memory.MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 5.1 KB, free 93.0 MB)
21/03/14 05:31:08 INFO storage.BlockManagerInfo: Added broadcast_2_piece0 in memory on master:41718 (size: 5.1 KB, free: 93.3 MB)
21/03/14 05:31:08 INFO spark.SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1161
21/03/14 05:31:08 INFO scheduler.DAGScheduler: Submitting 11 missing tasks from ResultStage 1 (PythonRDD[6] at collect at /home/user/ergasia/meros1/q2rdd.py:27) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
21/03/14 05:31:08 INFO scheduler.TaskSchedulerImpl: Adding task set 1.0 with 11 tasks
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 1.0 (TID 11, 192.168.0.1, executor 0, partition 0, NODE_LOCAL, 7666 bytes)
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 1.0 (TID 12, 192.168.0.2, executor 1, partition 1, NODE_LOCAL, 7666 bytes)
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Starting task 2.0 in stage 1.0 (TID 13, 192.168.0.1, executor 0, partition 2, NODE_LOCAL, 7666 bytes)
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Starting task 3.0 in stage 1.0 (TID 14, 192.168.0.2, executor 1, partition 3, NODE_LOCAL, 7666 bytes)
21/03/14 05:31:08 INFO storage.BlockManagerInfo: Added broadcast_2_piece0 in memory on 192.168.0.2:42175 (size: 5.1 KB, free: 1458.6 MB)
21/03/14 05:31:08 INFO storage.BlockManagerInfo: Added broadcast_2_piece0 in memory on 192.168.0.1:37240 (size: 5.1 KB, free: 1458.6 MB)
21/03/14 05:31:08 INFO spark.MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 0 to 192.168.0.2:45410
21/03/14 05:31:08 INFO spark.MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 0 to 192.168.0.1:34474
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Starting task 4.0 in stage 1.0 (TID 15, 192.168.0.2, executor 1, partition 4, NODE_LOCAL, 7666 bytes)
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Starting task 5.0 in stage 1.0 (TID 16, 192.168.0.2, executor 1, partition 5, NODE_LOCAL, 7666 bytes)
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 1.0 (TID 12) in 351 ms on 192.168.0.2 (executor 1) (1/11)
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Finished task 3.0 in stage 1.0 (TID 14) in 351 ms on 192.168.0.2 (executor 1) (2/11)
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Starting task 6.0 in stage 1.0 (TID 17, 192.168.0.1, executor 0, partition 6, NODE_LOCAL, 7666 bytes)
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Finished task 2.0 in stage 1.0 (TID 13) in 373 ms on 192.168.0.1 (executor 0) (3/11)
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Starting task 7.0 in stage 1.0 (TID 18, 192.168.0.1, executor 0, partition 7, NODE_LOCAL, 7666 bytes)
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 1.0 (TID 11) in 388 ms on 192.168.0.1 (executor 0) (4/11)
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Starting task 8.0 in stage 1.0 (TID 19, 192.168.0.2, executor 1, partition 8, NODE_LOCAL, 7666 bytes)
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Finished task 4.0 in stage 1.0 (TID 15) in 183 ms on 192.168.0.2 (executor 1) (5/11)
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Starting task 9.0 in stage 1.0 (TID 20, 192.168.0.1, executor 0, partition 9, NODE_LOCAL, 7666 bytes)
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Finished task 6.0 in stage 1.0 (TID 17) in 185 ms on 192.168.0.1 (executor 0) (6/11)
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Starting task 10.0 in stage 1.0 (TID 21, 192.168.0.2, executor 1, partition 10, NODE_LOCAL, 7666 bytes)
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Finished task 5.0 in stage 1.0 (TID 16) in 236 ms on 192.168.0.2 (executor 1) (7/11)
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Finished task 7.0 in stage 1.0 (TID 18) in 256 ms on 192.168.0.1 (executor 0) (8/11)
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Finished task 8.0 in stage 1.0 (TID 19) in 135 ms on 192.168.0.2 (executor 1) (9/11)
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Finished task 9.0 in stage 1.0 (TID 20) in 173 ms on 192.168.0.1 (executor 0) (10/11)
21/03/14 05:31:08 INFO scheduler.TaskSetManager: Finished task 10.0 in stage 1.0 (TID 21) in 165 ms on 192.168.0.2 (executor 1) (11/11)
21/03/14 05:31:08 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
21/03/14 05:31:08 INFO scheduler.DAGScheduler: ResultStage 1 (collect at /home/user/ergasia/meros1/q2rdd.py:27) finished in 0.767 s
21/03/14 05:31:08 INFO scheduler.DAGScheduler: Job 0 finished: collect at /home/user/ergasia/meros1/q2rdd.py:27, took 57.388800 s
21/03/14 05:31:08 INFO storage.BlockManagerInfo: Removed broadcast_1_piece0 on 192.168.0.2:42175 in memory (size: 7.4 KB, free: 1458.6 MB)
21/03/14 05:31:08 INFO storage.BlockManagerInfo: Removed broadcast_1_piece0 on 192.168.0.1:37240 in memory (size: 7.4 KB, free: 1458.6 MB)
21/03/14 05:31:08 INFO storage.BlockManagerInfo: Removed broadcast_1_piece0 on master:41718 in memory (size: 7.4 KB, free: 93.3 MB)
Number of users with mean ratings > 3 : 0.8747858956942886
Time of Q1 using Map-Reduce with csv is: 59.95288586616516 seconds
21/03/14 05:31:09 INFO spark.SparkContext: Invoking stop() from shutdown hook
21/03/14 05:31:09 INFO server.AbstractConnector: Stopped Spark@235c0abc{HTTP/1.1,[http/1.1]}{0.0.0.0:4040}
21/03/14 05:31:09 INFO ui.SparkUI: Stopped Spark web UI at http://master:4040
21/03/14 05:31:09 INFO cluster.StandaloneSchedulerBackend: Shutting down all executors
21/03/14 05:31:09 INFO cluster.CoarseGrainedSchedulerBackend$DriverEndpoint: Asking each executor to shut down
21/03/14 05:31:09 INFO spark.MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
21/03/14 05:31:09 INFO memory.MemoryStore: MemoryStore cleared
21/03/14 05:31:09 INFO storage.BlockManager: BlockManager stopped
21/03/14 05:31:09 INFO storage.BlockManagerMaster: BlockManagerMaster stopped
21/03/14 05:31:09 INFO scheduler.OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
21/03/14 05:31:09 INFO spark.SparkContext: Successfully stopped SparkContext
21/03/14 05:31:09 INFO util.ShutdownHookManager: Shutdown hook called
21/03/14 05:31:09 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-500f8504-66d1-46f6-953b-6f9374a6f439
21/03/14 05:31:09 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-4727e6a0-7e6c-4592-8087-2c94682d1d39
21/03/14 05:31:09 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-4727e6a0-7e6c-4592-8087-2c94682d1d39/pyspark-c49e5db0-d3fa-4f25-8fd5-b22b00a9406a
]0;user@master: ~/ergasia/meros1user@master:~/ergasia/meros1$ spark-submit q2rdd.py P[Kexit
exit

Script done on Sun 14 Mar 2021 05:31:17 AM EET
