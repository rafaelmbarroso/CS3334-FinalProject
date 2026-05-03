## Scope of project:

### Required files: 
- Task Dispatcher: creates and sends tasks
    - Distrubute between 70% I/O and 30% CPU tasks
    - Task types: 
        - I/O sleep(200ms)
        - 10% CPU variable
        - Global CPU cap at 100%
        - CPU Sleep will be around 35% of CPU time

- Manager Queue: Receives tasks and thinks about what to send to the workers
    - Do we have enough CPU resources? (check CPU availability)
    - Do I have enough workers? (check available workers)
    - Reminder, cap global CPU usage at 100%

- Worker Pool:
    - Create 8 worker threads, receiving the following task types: 
        - 1) I/O (a 200ms sleep), always uses 10% of CPU
        - 2) CPU (a 200ms sleep), always uses 35% of CPU

Monitor: Thread independent, checks current workl;oad and records in 10ms time intervals, mainly logging

### Overview:
- Threads to do: 
    - 1 main 
    - 1 monitor 
    - 8 workers 
    - Maanager queue of 8 workers

### Simulations:
- 2 smins, FIFO approach

