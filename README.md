# reliable-execution
Implement reliable execution with message queue

I. Summary

![Alt text](docs/main.png?raw=true "implementation")


II. Details

  This is demo using queue redis-pubsub, call to external service randomly success or fail by timestamp

  Build state machine to control a execution 

  Build API to submit task and polling status by request_id


II. Statue machine 

![Alt text](docs/state_machine.png?raw=true "implementation")

