### Install RabbitMQ on docker
> docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4.0-management

Note: 
1. install the image which has management plugin so as to launch UI Managemnent
2. --rm will remove the container after container is exited/stopped.
3. rabbitmq:latest != rabbitmq:management. The later one (pulls latest version &) has management UI plugin enabled which helps enable website view of RMQ
4. http://localhost:15672/ launches RMQ UI [(username, pwd) = (guest, guest)]

### References
* https://www.youtube.com/playlist?list=PL1xVF1dBM4bnc-NeY-yBMvJuKXY6IY9yP
* https://www.rabbitmq.com/docs/download#java-client
* Hello world, Work queues, Pub-Sub, Routing, topics, RPC, Publisher confirms: https://www.rabbitmq.com/tutorials/tutorial-one-java
* https://stackoverflow.com/questions/47290108/how-to-open-rabbitmq-in-browser-using-docker-container#:~:text=You%20are%20using%20the%20wrong%20image%20which%20doesn%27t%20have%20the%20rabbitmq_management%20plugin%20enabled.%20Change%20rabbitmq%3Alatest%20to%20rabbitmq%3Amanagement.
* https://stackoverflow.com/questions/21248563/rabbitmq-difference-between-exclusive-and-auto-delete
* https://www.geeksforgeeks.org/what-is-pub-sub/
