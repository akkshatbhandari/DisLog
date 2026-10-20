# DisLog
This is scalable distributed log and even analytics platform

Stage 1: receive events through Node.js, validate them in FastAPI, and persist them in Neon PostgreSQL.

Stage 2: RabbitMQ vs Kafka discussion

Stage 3: Kafka ingestion pipeline and producer consumer api integration

Stage 4: Dead Letter Queue handling

Stage 5: Offset tracking and Bounded work queue

Stage 6: Retries, failure handling

Stage 7: Batch processing of event streams by consumers for persistence

### Batch Persistence Performance Evaluation

We evaluated the DisLog ingestion service using 1,000 events stored in Neon PostgreSQL. The individual-insert implementation completed the workload in 370.75 seconds, achieving a throughput of approximately 2.7 events per second. The batch implementation, configured with a batch size of 25, completed the same workload in 15.03 seconds, achieving approximately 66.5 events per second.

The measured results demonstrate a **24.67× speedup** and approximately **95.9% lower execution time** for the batch implementation. The expected number of database transactions decreased from 1,000 to 40, representing a 96% reduction, assuming successful operations without retries.

These results indicate that batching can substantially improve database-write throughput for DisLog under the tested conditions. The measurements reflect this particular workload and database environment; repeated runs and additional batch sizes are needed to establish how consistently the improvement generalizes.


