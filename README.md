#### About timed executor

Timed executor is a service that accepts events as pairs of (`LocalDateTime`, `Callable`) and executes each event's `Callable` not earlier than the specified `LocalDateTime`. 
The tasks should be executed according to the value of the `LocalDateTime` or in the order of events' arrival if their `LocalDateTime`s are equal. Events may arrive in arbitrary order 
and be enqueued from different threads.
Wittingly overdue events are not accepted and no events are being timed out. It is assumed that there will be enough available resources for supporting any possible system backlog.