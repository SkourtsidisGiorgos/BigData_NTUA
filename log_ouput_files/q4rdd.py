Script started on Sun 14 Mar 2021 05:33:27 AM EET
]0;user@master: ~/ergasia/meros1user@master:~/ergasia/meros1$ exitspark-submit q3rdd.py exit[Kspark-submit q3rdd.py [1P[1@4
21/03/14 05:33:37 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
21/03/14 05:33:38 INFO spark.SparkContext: Running Spark version 2.4.4
21/03/14 05:33:38 INFO spark.SparkContext: Submitted application: Q4_RDD
21/03/14 05:33:38 INFO spark.SecurityManager: Changing view acls to: user
21/03/14 05:33:38 INFO spark.SecurityManager: Changing modify acls to: user
21/03/14 05:33:38 INFO spark.SecurityManager: Changing view acls groups to: 
21/03/14 05:33:38 INFO spark.SecurityManager: Changing modify acls groups to: 
21/03/14 05:33:38 INFO spark.SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(user); groups with view permissions: Set(); users  with modify permissions: Set(user); groups with modify permissions: Set()
21/03/14 05:33:38 INFO util.Utils: Successfully started service 'sparkDriver' on port 46424.
21/03/14 05:33:38 INFO spark.SparkEnv: Registering MapOutputTracker
21/03/14 05:33:38 INFO spark.SparkEnv: Registering BlockManagerMaster
21/03/14 05:33:38 INFO storage.BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
21/03/14 05:33:38 INFO storage.BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
21/03/14 05:33:38 INFO storage.DiskBlockManager: Created local directory at /tmp/blockmgr-5efa4b15-31f6-4b3a-8cc8-76c91e559999
21/03/14 05:33:38 INFO memory.MemoryStore: MemoryStore started with capacity 93.3 MB
21/03/14 05:33:38 INFO spark.SparkEnv: Registering OutputCommitCoordinator
21/03/14 05:33:38 INFO util.log: Logging initialized @2608ms
21/03/14 05:33:38 INFO server.Server: jetty-9.3.z-SNAPSHOT, build timestamp: unknown, git hash: unknown
21/03/14 05:33:38 INFO server.Server: Started @2684ms
21/03/14 05:33:38 INFO server.AbstractConnector: Started ServerConnector@79dd1f9e{HTTP/1.1,[http/1.1]}{0.0.0.0:4040}
21/03/14 05:33:38 INFO util.Utils: Successfully started service 'SparkUI' on port 4040.
21/03/14 05:33:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@5b61ca17{/jobs,null,AVAILABLE,@Spark}
21/03/14 05:33:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@b7e2717{/jobs/json,null,AVAILABLE,@Spark}
21/03/14 05:33:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@48e33bed{/jobs/job,null,AVAILABLE,@Spark}
21/03/14 05:33:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@47931ea6{/jobs/job/json,null,AVAILABLE,@Spark}
21/03/14 05:33:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@3dafcfdc{/stages,null,AVAILABLE,@Spark}
21/03/14 05:33:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@6cc2a7d7{/stages/json,null,AVAILABLE,@Spark}
21/03/14 05:33:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@7044a52{/stages/stage,null,AVAILABLE,@Spark}
21/03/14 05:33:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@62aa0773{/stages/stage/json,null,AVAILABLE,@Spark}
21/03/14 05:33:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@475987ae{/stages/pool,null,AVAILABLE,@Spark}
21/03/14 05:33:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@77c571bd{/stages/pool/json,null,AVAILABLE,@Spark}
21/03/14 05:33:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@13b0798c{/storage,null,AVAILABLE,@Spark}
21/03/14 05:33:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@21137516{/storage/json,null,AVAILABLE,@Spark}
21/03/14 05:33:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@2a7ce26d{/storage/rdd,null,AVAILABLE,@Spark}
21/03/14 05:33:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@76683606{/storage/rdd/json,null,AVAILABLE,@Spark}
21/03/14 05:33:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@66845d5e{/environment,null,AVAILABLE,@Spark}
21/03/14 05:33:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@5c853f5d{/environment/json,null,AVAILABLE,@Spark}
21/03/14 05:33:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@1127d12a{/executors,null,AVAILABLE,@Spark}
21/03/14 05:33:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@26c81d0e{/executors/json,null,AVAILABLE,@Spark}
21/03/14 05:33:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@6262ca24{/executors/threadDump,null,AVAILABLE,@Spark}
21/03/14 05:33:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@17686ee2{/executors/threadDump/json,null,AVAILABLE,@Spark}
21/03/14 05:33:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@386943a4{/static,null,AVAILABLE,@Spark}
21/03/14 05:33:38 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@40d06d98{/,null,AVAILABLE,@Spark}
21/03/14 05:33:39 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@6869d4ac{/api,null,AVAILABLE,@Spark}
21/03/14 05:33:39 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@1c42528e{/jobs/job/kill,null,AVAILABLE,@Spark}
21/03/14 05:33:39 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@38cffe5d{/stages/stage/kill,null,AVAILABLE,@Spark}
21/03/14 05:33:39 INFO ui.SparkUI: Bound SparkUI to 0.0.0.0, and started at http://master:4040
21/03/14 05:33:39 INFO client.StandaloneAppClient$ClientEndpoint: Connecting to master spark://master:7077...
21/03/14 05:33:39 INFO client.TransportClientFactory: Successfully created connection to master/192.168.0.1:7077 after 42 ms (0 ms spent in bootstraps)
21/03/14 05:33:39 INFO cluster.StandaloneSchedulerBackend: Connected to Spark cluster with app ID app-20210314053339-0100
21/03/14 05:33:39 INFO client.StandaloneAppClient$ClientEndpoint: Executor added: app-20210314053339-0100/0 on worker-20210313132157-192.168.0.1-43826 (192.168.0.1:43826) with 2 core(s)
21/03/14 05:33:39 INFO cluster.StandaloneSchedulerBackend: Granted executor ID app-20210314053339-0100/0 on hostPort 192.168.0.1:43826 with 2 core(s), 3.0 GB RAM
21/03/14 05:33:39 INFO client.StandaloneAppClient$ClientEndpoint: Executor added: app-20210314053339-0100/1 on worker-20210313132158-192.168.0.2-46469 (192.168.0.2:46469) with 2 core(s)
21/03/14 05:33:39 INFO cluster.StandaloneSchedulerBackend: Granted executor ID app-20210314053339-0100/1 on hostPort 192.168.0.2:46469 with 2 core(s), 3.0 GB RAM
21/03/14 05:33:39 INFO util.Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 35395.
21/03/14 05:33:39 INFO netty.NettyBlockTransferService: Server created on master:35395
21/03/14 05:33:39 INFO storage.BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
21/03/14 05:33:39 INFO client.StandaloneAppClient$ClientEndpoint: Executor updated: app-20210314053339-0100/1 is now RUNNING
21/03/14 05:33:39 INFO client.StandaloneAppClient$ClientEndpoint: Executor updated: app-20210314053339-0100/0 is now RUNNING
21/03/14 05:33:39 INFO storage.BlockManagerMaster: Registering BlockManager BlockManagerId(driver, master, 35395, None)
21/03/14 05:33:39 INFO storage.BlockManagerMasterEndpoint: Registering block manager master:35395 with 93.3 MB RAM, BlockManagerId(driver, master, 35395, None)
21/03/14 05:33:39 INFO storage.BlockManagerMaster: Registered BlockManager BlockManagerId(driver, master, 35395, None)
21/03/14 05:33:39 INFO storage.BlockManager: Initialized BlockManager: BlockManagerId(driver, master, 35395, None)
21/03/14 05:33:39 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@43669d1b{/metrics/json,null,AVAILABLE,@Spark}
21/03/14 05:33:39 INFO cluster.StandaloneSchedulerBackend: SchedulerBackend is ready for scheduling beginning after reached minRegisteredResourcesRatio: 0.0
21/03/14 05:33:40 INFO internal.SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir ('file:/home/user/ergasia/meros1/spark-warehouse/').
21/03/14 05:33:40 INFO internal.SharedState: Warehouse path is 'file:/home/user/ergasia/meros1/spark-warehouse/'.
21/03/14 05:33:40 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@625532d6{/SQL,null,AVAILABLE,@Spark}
21/03/14 05:33:40 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@5db3e2a9{/SQL/json,null,AVAILABLE,@Spark}
21/03/14 05:33:40 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@4356e512{/SQL/execution,null,AVAILABLE,@Spark}
21/03/14 05:33:40 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@371d8361{/SQL/execution/json,null,AVAILABLE,@Spark}
21/03/14 05:33:40 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@30a691c2{/static/sql,null,AVAILABLE,@Spark}
21/03/14 05:33:41 INFO state.StateStoreCoordinatorRef: Registered StateStoreCoordinator endpoint
21/03/14 05:33:41 INFO cluster.CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (192.168.0.2:54354) with ID 1
21/03/14 05:33:41 INFO storage.BlockManagerMasterEndpoint: Registering block manager 192.168.0.2:42118 with 1458.6 MB RAM, BlockManagerId(1, 192.168.0.2, 42118, None)
21/03/14 05:33:42 INFO memory.MemoryStore: Block broadcast_0 stored as values in memory (estimated size 239.4 KB, free 93.1 MB)
21/03/14 05:33:42 INFO memory.MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 23.1 KB, free 93.0 MB)
21/03/14 05:33:42 INFO storage.BlockManagerInfo: Added broadcast_0_piece0 in memory on master:35395 (size: 23.1 KB, free: 93.3 MB)
21/03/14 05:33:42 INFO spark.SparkContext: Created broadcast 0 from textFile at NativeMethodAccessorImpl.java:0
21/03/14 05:33:42 INFO memory.MemoryStore: Block broadcast_1 stored as values in memory (estimated size 239.5 KB, free 92.8 MB)
21/03/14 05:33:42 INFO memory.MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 23.1 KB, free 92.8 MB)
21/03/14 05:33:42 INFO storage.BlockManagerInfo: Added broadcast_1_piece0 in memory on master:35395 (size: 23.1 KB, free: 93.3 MB)
21/03/14 05:33:42 INFO spark.SparkContext: Created broadcast 1 from textFile at NativeMethodAccessorImpl.java:0
21/03/14 05:33:43 INFO mapred.FileInputFormat: Total input paths to process : 1
21/03/14 05:33:43 INFO cluster.CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (192.168.0.1:37940) with ID 0
21/03/14 05:33:43 INFO mapred.FileInputFormat: Total input paths to process : 1
21/03/14 05:33:43 INFO storage.BlockManagerMasterEndpoint: Registering block manager 192.168.0.1:41575 with 1458.6 MB RAM, BlockManagerId(0, 192.168.0.1, 41575, None)
21/03/14 05:33:43 INFO spark.SparkContext: Starting job: sortByKey at /home/user/ergasia/meros1/q4rdd.py:50
21/03/14 05:33:43 INFO scheduler.DAGScheduler: Registering RDD 8 (join at /home/user/ergasia/meros1/q4rdd.py:45)
21/03/14 05:33:43 INFO scheduler.DAGScheduler: Registering RDD 12 (reduceByKey at /home/user/ergasia/meros1/q4rdd.py:48)
21/03/14 05:33:43 INFO scheduler.DAGScheduler: Got job 0 (sortByKey at /home/user/ergasia/meros1/q4rdd.py:50) with 4 output partitions
21/03/14 05:33:43 INFO scheduler.DAGScheduler: Final stage: ResultStage 2 (sortByKey at /home/user/ergasia/meros1/q4rdd.py:50)
21/03/14 05:33:43 INFO scheduler.DAGScheduler: Parents of final stage: List(ShuffleMapStage 1)
21/03/14 05:33:43 INFO scheduler.DAGScheduler: Missing parents: List(ShuffleMapStage 1)
21/03/14 05:33:44 INFO scheduler.DAGScheduler: Submitting ShuffleMapStage 0 (PairwiseRDD[8] at join at /home/user/ergasia/meros1/q4rdd.py:45), which has no missing parents
21/03/14 05:33:44 INFO memory.MemoryStore: Block broadcast_2 stored as values in memory (estimated size 17.1 KB, free 92.8 MB)
21/03/14 05:33:44 INFO memory.MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 9.3 KB, free 92.8 MB)
21/03/14 05:33:44 INFO storage.BlockManagerInfo: Added broadcast_2_piece0 in memory on master:35395 (size: 9.3 KB, free: 93.2 MB)
21/03/14 05:33:44 INFO spark.SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1161
21/03/14 05:33:44 INFO scheduler.DAGScheduler: Submitting 4 missing tasks from ShuffleMapStage 0 (PairwiseRDD[8] at join at /home/user/ergasia/meros1/q4rdd.py:45) (first 15 tasks are for partitions Vector(0, 1, 2, 3))
21/03/14 05:33:44 INFO scheduler.TaskSchedulerImpl: Adding task set 0.0 with 4 tasks
21/03/14 05:33:44 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, 192.168.0.1, executor 0, partition 0, ANY, 7997 bytes)
21/03/14 05:33:44 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1, 192.168.0.2, executor 1, partition 1, ANY, 7997 bytes)
21/03/14 05:33:44 INFO scheduler.TaskSetManager: Starting task 2.0 in stage 0.0 (TID 2, 192.168.0.1, executor 0, partition 2, ANY, 7991 bytes)
21/03/14 05:33:44 INFO scheduler.TaskSetManager: Starting task 3.0 in stage 0.0 (TID 3, 192.168.0.2, executor 1, partition 3, ANY, 7991 bytes)
21/03/14 05:33:44 INFO storage.BlockManagerInfo: Added broadcast_2_piece0 in memory on 192.168.0.2:42118 (size: 9.3 KB, free: 1458.6 MB)
21/03/14 05:33:44 INFO storage.BlockManagerInfo: Added broadcast_2_piece0 in memory on 192.168.0.1:41575 (size: 9.3 KB, free: 1458.6 MB)
21/03/14 05:33:44 INFO storage.BlockManagerInfo: Added broadcast_0_piece0 in memory on 192.168.0.2:42118 (size: 23.1 KB, free: 1458.6 MB)
21/03/14 05:33:44 INFO storage.BlockManagerInfo: Added broadcast_1_piece0 in memory on 192.168.0.1:41575 (size: 23.1 KB, free: 1458.6 MB)
21/03/14 05:33:44 INFO storage.BlockManagerInfo: Added broadcast_1_piece0 in memory on 192.168.0.2:42118 (size: 23.1 KB, free: 1458.5 MB)
21/03/14 05:33:44 INFO storage.BlockManagerInfo: Added broadcast_0_piece0 in memory on 192.168.0.1:41575 (size: 23.1 KB, free: 1458.5 MB)
21/03/14 05:33:47 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 3105 ms on 192.168.0.2 (executor 1) (1/4)
21/03/14 05:33:47 INFO python.PythonAccumulatorV2: Connected to AccumulatorServer at host: 127.0.0.1 port: 53074
21/03/14 05:33:47 INFO scheduler.TaskSetManager: Finished task 3.0 in stage 0.0 (TID 3) in 3234 ms on 192.168.0.2 (executor 1) (2/4)
21/03/14 05:33:47 INFO scheduler.TaskSetManager: Finished task 2.0 in stage 0.0 (TID 2) in 3481 ms on 192.168.0.1 (executor 0) (3/4)
21/03/14 05:33:47 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 3518 ms on 192.168.0.1 (executor 0) (4/4)
21/03/14 05:33:47 INFO scheduler.DAGScheduler: ShuffleMapStage 0 (join at /home/user/ergasia/meros1/q4rdd.py:45) finished in 3.587 s
21/03/14 05:33:47 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
21/03/14 05:33:47 INFO scheduler.DAGScheduler: looking for newly runnable stages
21/03/14 05:33:47 INFO scheduler.DAGScheduler: running: Set()
21/03/14 05:33:47 INFO scheduler.DAGScheduler: waiting: Set(ShuffleMapStage 1, ResultStage 2)
21/03/14 05:33:47 INFO scheduler.DAGScheduler: failed: Set()
21/03/14 05:33:47 INFO scheduler.DAGScheduler: Submitting ShuffleMapStage 1 (PairwiseRDD[12] at reduceByKey at /home/user/ergasia/meros1/q4rdd.py:48), which has no missing parents
21/03/14 05:33:47 INFO memory.MemoryStore: Block broadcast_3 stored as values in memory (estimated size 13.7 KB, free 92.7 MB)
21/03/14 05:33:47 INFO memory.MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 8.8 KB, free 92.7 MB)
21/03/14 05:33:47 INFO storage.BlockManagerInfo: Added broadcast_3_piece0 in memory on master:35395 (size: 8.8 KB, free: 93.2 MB)
21/03/14 05:33:47 INFO spark.SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1161
21/03/14 05:33:47 INFO scheduler.DAGScheduler: Submitting 4 missing tasks from ShuffleMapStage 1 (PairwiseRDD[12] at reduceByKey at /home/user/ergasia/meros1/q4rdd.py:48) (first 15 tasks are for partitions Vector(0, 1, 2, 3))
21/03/14 05:33:47 INFO scheduler.TaskSchedulerImpl: Adding task set 1.0 with 4 tasks
21/03/14 05:33:47 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 1.0 (TID 4, 192.168.0.2, executor 1, partition 0, NODE_LOCAL, 7655 bytes)
21/03/14 05:33:47 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 1.0 (TID 5, 192.168.0.1, executor 0, partition 1, NODE_LOCAL, 7655 bytes)
21/03/14 05:33:47 INFO scheduler.TaskSetManager: Starting task 2.0 in stage 1.0 (TID 6, 192.168.0.2, executor 1, partition 2, NODE_LOCAL, 7655 bytes)
21/03/14 05:33:47 INFO scheduler.TaskSetManager: Starting task 3.0 in stage 1.0 (TID 7, 192.168.0.1, executor 0, partition 3, NODE_LOCAL, 7655 bytes)
21/03/14 05:33:47 INFO storage.BlockManagerInfo: Added broadcast_3_piece0 in memory on 192.168.0.2:42118 (size: 8.8 KB, free: 1458.5 MB)
21/03/14 05:33:47 INFO spark.MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 1 to 192.168.0.2:54354
21/03/14 05:33:47 INFO storage.BlockManagerInfo: Added broadcast_3_piece0 in memory on 192.168.0.1:41575 (size: 8.8 KB, free: 1458.5 MB)
21/03/14 05:33:47 INFO spark.MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 1 to 192.168.0.1:37940
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Finished task 2.0 in stage 1.0 (TID 6) in 326 ms on 192.168.0.2 (executor 1) (1/4)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 1.0 (TID 4) in 357 ms on 192.168.0.2 (executor 1) (2/4)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 1.0 (TID 5) in 482 ms on 192.168.0.1 (executor 0) (3/4)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Finished task 3.0 in stage 1.0 (TID 7) in 487 ms on 192.168.0.1 (executor 0) (4/4)
21/03/14 05:33:48 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
21/03/14 05:33:48 INFO scheduler.DAGScheduler: ShuffleMapStage 1 (reduceByKey at /home/user/ergasia/meros1/q4rdd.py:48) finished in 0.502 s
21/03/14 05:33:48 INFO scheduler.DAGScheduler: looking for newly runnable stages
21/03/14 05:33:48 INFO scheduler.DAGScheduler: running: Set()
21/03/14 05:33:48 INFO scheduler.DAGScheduler: waiting: Set(ResultStage 2)
21/03/14 05:33:48 INFO scheduler.DAGScheduler: failed: Set()
21/03/14 05:33:48 INFO scheduler.DAGScheduler: Submitting ResultStage 2 (PythonRDD[15] at sortByKey at /home/user/ergasia/meros1/q4rdd.py:50), which has no missing parents
21/03/14 05:33:48 INFO memory.MemoryStore: Block broadcast_4 stored as values in memory (estimated size 9.3 KB, free 92.7 MB)
21/03/14 05:33:48 INFO memory.MemoryStore: Block broadcast_4_piece0 stored as bytes in memory (estimated size 6.0 KB, free 92.7 MB)
21/03/14 05:33:48 INFO storage.BlockManagerInfo: Added broadcast_4_piece0 in memory on master:35395 (size: 6.0 KB, free: 93.2 MB)
21/03/14 05:33:48 INFO spark.SparkContext: Created broadcast 4 from broadcast at DAGScheduler.scala:1161
21/03/14 05:33:48 INFO scheduler.DAGScheduler: Submitting 4 missing tasks from ResultStage 2 (PythonRDD[15] at sortByKey at /home/user/ergasia/meros1/q4rdd.py:50) (first 15 tasks are for partitions Vector(0, 1, 2, 3))
21/03/14 05:33:48 INFO scheduler.TaskSchedulerImpl: Adding task set 2.0 with 4 tasks
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Starting task 2.0 in stage 2.0 (TID 8, 192.168.0.1, executor 0, partition 2, NODE_LOCAL, 7666 bytes)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Starting task 3.0 in stage 2.0 (TID 9, 192.168.0.2, executor 1, partition 3, NODE_LOCAL, 7666 bytes)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 2.0 (TID 10, 192.168.0.1, executor 0, partition 0, PROCESS_LOCAL, 7666 bytes)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 2.0 (TID 11, 192.168.0.2, executor 1, partition 1, PROCESS_LOCAL, 7666 bytes)
21/03/14 05:33:48 INFO storage.BlockManagerInfo: Added broadcast_4_piece0 in memory on 192.168.0.2:42118 (size: 6.0 KB, free: 1458.5 MB)
21/03/14 05:33:48 INFO storage.BlockManagerInfo: Added broadcast_4_piece0 in memory on 192.168.0.1:41575 (size: 6.0 KB, free: 1458.5 MB)
21/03/14 05:33:48 INFO spark.MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 0 to 192.168.0.2:54354
21/03/14 05:33:48 INFO spark.MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 0 to 192.168.0.1:37940
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Finished task 3.0 in stage 2.0 (TID 9) in 106 ms on 192.168.0.2 (executor 1) (1/4)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 2.0 (TID 10) in 115 ms on 192.168.0.1 (executor 0) (2/4)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 2.0 (TID 11) in 116 ms on 192.168.0.2 (executor 1) (3/4)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Finished task 2.0 in stage 2.0 (TID 8) in 130 ms on 192.168.0.1 (executor 0) (4/4)
21/03/14 05:33:48 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 2.0, whose tasks have all completed, from pool 
21/03/14 05:33:48 INFO scheduler.DAGScheduler: ResultStage 2 (sortByKey at /home/user/ergasia/meros1/q4rdd.py:50) finished in 0.146 s
21/03/14 05:33:48 INFO scheduler.DAGScheduler: Job 0 finished: sortByKey at /home/user/ergasia/meros1/q4rdd.py:50, took 4.538862 s
21/03/14 05:33:48 INFO spark.SparkContext: Starting job: sortByKey at /home/user/ergasia/meros1/q4rdd.py:50
21/03/14 05:33:48 INFO scheduler.DAGScheduler: Got job 1 (sortByKey at /home/user/ergasia/meros1/q4rdd.py:50) with 4 output partitions
21/03/14 05:33:48 INFO scheduler.DAGScheduler: Final stage: ResultStage 5 (sortByKey at /home/user/ergasia/meros1/q4rdd.py:50)
21/03/14 05:33:48 INFO scheduler.DAGScheduler: Parents of final stage: List(ShuffleMapStage 4)
21/03/14 05:33:48 INFO scheduler.DAGScheduler: Missing parents: List()
21/03/14 05:33:48 INFO scheduler.DAGScheduler: Submitting ResultStage 5 (PythonRDD[16] at sortByKey at /home/user/ergasia/meros1/q4rdd.py:50), which has no missing parents
21/03/14 05:33:48 INFO memory.MemoryStore: Block broadcast_5 stored as values in memory (estimated size 9.1 KB, free 92.7 MB)
21/03/14 05:33:48 INFO memory.MemoryStore: Block broadcast_5_piece0 stored as bytes in memory (estimated size 5.9 KB, free 92.7 MB)
21/03/14 05:33:48 INFO storage.BlockManagerInfo: Added broadcast_5_piece0 in memory on master:35395 (size: 5.9 KB, free: 93.2 MB)
21/03/14 05:33:48 INFO spark.SparkContext: Created broadcast 5 from broadcast at DAGScheduler.scala:1161
21/03/14 05:33:48 INFO scheduler.DAGScheduler: Submitting 4 missing tasks from ResultStage 5 (PythonRDD[16] at sortByKey at /home/user/ergasia/meros1/q4rdd.py:50) (first 15 tasks are for partitions Vector(0, 1, 2, 3))
21/03/14 05:33:48 INFO scheduler.TaskSchedulerImpl: Adding task set 5.0 with 4 tasks
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Starting task 2.0 in stage 5.0 (TID 12, 192.168.0.2, executor 1, partition 2, NODE_LOCAL, 7666 bytes)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Starting task 3.0 in stage 5.0 (TID 13, 192.168.0.1, executor 0, partition 3, NODE_LOCAL, 7666 bytes)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 5.0 (TID 14, 192.168.0.2, executor 1, partition 0, PROCESS_LOCAL, 7666 bytes)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 5.0 (TID 15, 192.168.0.1, executor 0, partition 1, PROCESS_LOCAL, 7666 bytes)
21/03/14 05:33:48 INFO storage.BlockManagerInfo: Added broadcast_5_piece0 in memory on 192.168.0.2:42118 (size: 5.9 KB, free: 1458.5 MB)
21/03/14 05:33:48 INFO storage.BlockManagerInfo: Added broadcast_5_piece0 in memory on 192.168.0.1:41575 (size: 5.9 KB, free: 1458.5 MB)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 5.0 (TID 14) in 68 ms on 192.168.0.2 (executor 1) (1/4)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Finished task 2.0 in stage 5.0 (TID 12) in 75 ms on 192.168.0.2 (executor 1) (2/4)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 5.0 (TID 15) in 99 ms on 192.168.0.1 (executor 0) (3/4)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Finished task 3.0 in stage 5.0 (TID 13) in 135 ms on 192.168.0.1 (executor 0) (4/4)
21/03/14 05:33:48 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 5.0, whose tasks have all completed, from pool 
21/03/14 05:33:48 INFO scheduler.DAGScheduler: ResultStage 5 (sortByKey at /home/user/ergasia/meros1/q4rdd.py:50) finished in 0.154 s
21/03/14 05:33:48 INFO scheduler.DAGScheduler: Job 1 finished: sortByKey at /home/user/ergasia/meros1/q4rdd.py:50, took 0.159842 s
21/03/14 05:33:48 INFO spark.SparkContext: Starting job: collect at /home/user/ergasia/meros1/q4rdd.py:50
21/03/14 05:33:48 INFO scheduler.DAGScheduler: Registering RDD 18 (sortByKey at /home/user/ergasia/meros1/q4rdd.py:50)
21/03/14 05:33:48 INFO scheduler.DAGScheduler: Got job 2 (collect at /home/user/ergasia/meros1/q4rdd.py:50) with 4 output partitions
21/03/14 05:33:48 INFO scheduler.DAGScheduler: Final stage: ResultStage 9 (collect at /home/user/ergasia/meros1/q4rdd.py:50)
21/03/14 05:33:48 INFO scheduler.DAGScheduler: Parents of final stage: List(ShuffleMapStage 8)
21/03/14 05:33:48 INFO scheduler.DAGScheduler: Missing parents: List(ShuffleMapStage 8)
21/03/14 05:33:48 INFO scheduler.DAGScheduler: Submitting ShuffleMapStage 8 (PairwiseRDD[18] at sortByKey at /home/user/ergasia/meros1/q4rdd.py:50), which has no missing parents
21/03/14 05:33:48 INFO memory.MemoryStore: Block broadcast_6 stored as values in memory (estimated size 10.1 KB, free 92.7 MB)
21/03/14 05:33:48 INFO memory.MemoryStore: Block broadcast_6_piece0 stored as bytes in memory (estimated size 6.6 KB, free 92.7 MB)
21/03/14 05:33:48 INFO storage.BlockManagerInfo: Added broadcast_6_piece0 in memory on master:35395 (size: 6.6 KB, free: 93.2 MB)
21/03/14 05:33:48 INFO spark.SparkContext: Created broadcast 6 from broadcast at DAGScheduler.scala:1161
21/03/14 05:33:48 INFO scheduler.DAGScheduler: Submitting 4 missing tasks from ShuffleMapStage 8 (PairwiseRDD[18] at sortByKey at /home/user/ergasia/meros1/q4rdd.py:50) (first 15 tasks are for partitions Vector(0, 1, 2, 3))
21/03/14 05:33:48 INFO scheduler.TaskSchedulerImpl: Adding task set 8.0 with 4 tasks
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Starting task 2.0 in stage 8.0 (TID 16, 192.168.0.1, executor 0, partition 2, NODE_LOCAL, 7655 bytes)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Starting task 3.0 in stage 8.0 (TID 17, 192.168.0.2, executor 1, partition 3, NODE_LOCAL, 7655 bytes)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 8.0 (TID 18, 192.168.0.1, executor 0, partition 0, PROCESS_LOCAL, 7655 bytes)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 8.0 (TID 19, 192.168.0.2, executor 1, partition 1, PROCESS_LOCAL, 7655 bytes)
21/03/14 05:33:48 INFO storage.BlockManagerInfo: Added broadcast_6_piece0 in memory on 192.168.0.2:42118 (size: 6.6 KB, free: 1458.5 MB)
21/03/14 05:33:48 INFO storage.BlockManagerInfo: Added broadcast_6_piece0 in memory on 192.168.0.1:41575 (size: 6.6 KB, free: 1458.5 MB)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 8.0 (TID 18) in 86 ms on 192.168.0.1 (executor 0) (1/4)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 8.0 (TID 19) in 91 ms on 192.168.0.2 (executor 1) (2/4)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Finished task 3.0 in stage 8.0 (TID 17) in 113 ms on 192.168.0.2 (executor 1) (3/4)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Finished task 2.0 in stage 8.0 (TID 16) in 127 ms on 192.168.0.1 (executor 0) (4/4)
21/03/14 05:33:48 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 8.0, whose tasks have all completed, from pool 
21/03/14 05:33:48 INFO scheduler.DAGScheduler: ShuffleMapStage 8 (sortByKey at /home/user/ergasia/meros1/q4rdd.py:50) finished in 0.147 s
21/03/14 05:33:48 INFO scheduler.DAGScheduler: looking for newly runnable stages
21/03/14 05:33:48 INFO scheduler.DAGScheduler: running: Set()
21/03/14 05:33:48 INFO scheduler.DAGScheduler: waiting: Set(ResultStage 9)
21/03/14 05:33:48 INFO scheduler.DAGScheduler: failed: Set()
21/03/14 05:33:48 INFO scheduler.DAGScheduler: Submitting ResultStage 9 (PythonRDD[21] at collect at /home/user/ergasia/meros1/q4rdd.py:50), which has no missing parents
21/03/14 05:33:48 INFO memory.MemoryStore: Block broadcast_7 stored as values in memory (estimated size 6.6 KB, free 92.7 MB)
21/03/14 05:33:48 INFO memory.MemoryStore: Block broadcast_7_piece0 stored as bytes in memory (estimated size 4.2 KB, free 92.7 MB)
21/03/14 05:33:48 INFO storage.BlockManagerInfo: Added broadcast_7_piece0 in memory on master:35395 (size: 4.2 KB, free: 93.2 MB)
21/03/14 05:33:48 INFO spark.SparkContext: Created broadcast 7 from broadcast at DAGScheduler.scala:1161
21/03/14 05:33:48 INFO scheduler.DAGScheduler: Submitting 4 missing tasks from ResultStage 9 (PythonRDD[21] at collect at /home/user/ergasia/meros1/q4rdd.py:50) (first 15 tasks are for partitions Vector(0, 1, 2, 3))
21/03/14 05:33:48 INFO scheduler.TaskSchedulerImpl: Adding task set 9.0 with 4 tasks
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 9.0 (TID 20, 192.168.0.2, executor 1, partition 0, NODE_LOCAL, 7666 bytes)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 9.0 (TID 21, 192.168.0.1, executor 0, partition 1, NODE_LOCAL, 7666 bytes)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Starting task 2.0 in stage 9.0 (TID 22, 192.168.0.1, executor 0, partition 2, NODE_LOCAL, 7666 bytes)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Starting task 3.0 in stage 9.0 (TID 23, 192.168.0.2, executor 1, partition 3, PROCESS_LOCAL, 7666 bytes)
21/03/14 05:33:48 INFO storage.BlockManagerInfo: Added broadcast_7_piece0 in memory on 192.168.0.1:41575 (size: 4.2 KB, free: 1458.5 MB)
21/03/14 05:33:48 INFO spark.MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 2 to 192.168.0.1:37940
21/03/14 05:33:48 INFO storage.BlockManagerInfo: Added broadcast_7_piece0 in memory on 192.168.0.2:42118 (size: 4.2 KB, free: 1458.5 MB)
21/03/14 05:33:48 INFO spark.MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 2 to 192.168.0.2:54354
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 9.0 (TID 21) in 88 ms on 192.168.0.1 (executor 0) (1/4)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Finished task 2.0 in stage 9.0 (TID 22) in 103 ms on 192.168.0.1 (executor 0) (2/4)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Finished task 3.0 in stage 9.0 (TID 23) in 106 ms on 192.168.0.2 (executor 1) (3/4)
21/03/14 05:33:48 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 9.0 (TID 20) in 110 ms on 192.168.0.2 (executor 1) (4/4)
21/03/14 05:33:48 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 9.0, whose tasks have all completed, from pool 
21/03/14 05:33:48 INFO scheduler.DAGScheduler: ResultStage 9 (collect at /home/user/ergasia/meros1/q4rdd.py:50) finished in 0.123 s
21/03/14 05:33:48 INFO scheduler.DAGScheduler: Job 2 finished: collect at /home/user/ergasia/meros1/q4rdd.py:50, took 0.282435 s
Year    |   Average Summary Length 
('2000-2004', 57.95583449558345)
('2005-2009', 54.744193445752465)
('2010-2014', 57.37850729517396)
('2015-2019', 49.59975445058318)
Q1_RDD Execution time: 7.6572792530059814 seconds
21/03/14 05:33:48 INFO spark.SparkContext: Invoking stop() from shutdown hook
21/03/14 05:33:48 INFO server.AbstractConnector: Stopped Spark@79dd1f9e{HTTP/1.1,[http/1.1]}{0.0.0.0:4040}
21/03/14 05:33:48 INFO storage.BlockManagerInfo: Removed broadcast_3_piece0 on 192.168.0.2:42118 in memory (size: 8.8 KB, free: 1458.5 MB)
21/03/14 05:33:48 INFO ui.SparkUI: Stopped Spark web UI at http://master:4040
21/03/14 05:33:48 INFO storage.BlockManagerInfo: Removed broadcast_3_piece0 on 192.168.0.1:41575 in memory (size: 8.8 KB, free: 1458.5 MB)
21/03/14 05:33:48 INFO storage.BlockManagerInfo: Removed broadcast_3_piece0 on master:35395 in memory (size: 8.8 KB, free: 93.2 MB)
21/03/14 05:33:48 INFO cluster.StandaloneSchedulerBackend: Shutting down all executors
21/03/14 05:33:48 INFO cluster.CoarseGrainedSchedulerBackend$DriverEndpoint: Asking each executor to shut down
21/03/14 05:33:49 INFO spark.MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
21/03/14 05:33:49 WARN nio.NioEventLoop: Selector.select() returned prematurely 512 times in a row; rebuilding Selector io.netty.channel.nio.SelectedSelectionKeySetSelector@5539b932.
21/03/14 05:33:49 INFO nio.NioEventLoop: Migrated 2 channel(s) to the new Selector.
21/03/14 05:33:49 INFO memory.MemoryStore: MemoryStore cleared
21/03/14 05:33:49 INFO storage.BlockManager: BlockManager stopped
21/03/14 05:33:49 INFO storage.BlockManagerMaster: BlockManagerMaster stopped
21/03/14 05:33:49 INFO scheduler.OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
21/03/14 05:33:49 INFO spark.SparkContext: Successfully stopped SparkContext
21/03/14 05:33:49 INFO util.ShutdownHookManager: Shutdown hook called
21/03/14 05:33:49 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-8a538560-4cee-4ec0-8425-78a49ca73c31
21/03/14 05:33:49 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-c9068741-7eaf-484d-b171-bad3af3d49d2
21/03/14 05:33:49 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-8a538560-4cee-4ec0-8425-78a49ca73c31/pyspark-ac42d934-32fc-499c-ab8f-008073ec1783
]0;user@master: ~/ergasia/meros1user@master:~/ergasia/meros1$ exit
exit

Script done on Sun 14 Mar 2021 05:33:52 AM EET
