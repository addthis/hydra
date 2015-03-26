# Hydra Local Stack Container
Follow the instructions in this document to go from zero to a fully functional Hydra local stack environment.
 *Local Stack* is a environment that contains all of the services required to run and query Hydra jobs.
 The stack runs inside of [Docker](https://www.docker.com/) containers and Docker is a prequisite for getting
 your stack up and running.

##  Prerequisites

  - Docker 1.3.1 or greater.  If you are on a OSX you can use [boot2docker](http://boot2docker.io/)
  - [Maven](http://maven.apache.org/) 3.0 or higher
  - [Java 8](http://www.oracle.com/technetwork/java/javase/overview/java8-2100321.html) or higher]
 
## Installation

  - Clone and build hydra
 ```sh
 $ git clone https://github.com/addthis/hydra.git
 $ cd hydra
 $ mvn clean package -P bdbje
 ```
  - Make sure docker is up and running (reminder if you are on OSX you should use boot2docker)
```sh
$ docker version
Client version: 1.3.1
Client API version: 1.15
Go version (client): go1.3.3
Git commit (client): 4e9bbfa
OS/Arch (client): darwin/amd64
Server version: 1.3.1
Server API version: 1.15
Go version (server): go1.3.3
Git commit (server): 4e9bbfa
```
  - Build required docker containers
```sh
$ cd docker
$ ./docker.sh buildall
```

  - Start docker containers.  This will start three containers:  RabbitMQ, Zookeeper, and Hydra
```sh
$ ./docker.sh startall
```
  - Login to the Spawn UI to ensure everything started properly.  It can take a minute or two to startup.  
    - OSX - http://192.168.59.103:5052/spawn2/index.html
    - Linux - http://localhost:5052/spawn2/index.html
  - Once the Spawn UI loads initialze the environment using:
```sh
$ ./docker.sh init hydra
```
  - Create your first Hydra Job.  The command below creates a simple hydra job that randomly generates a number
  between 0 and 9 and does this one thousand times.  It counts how many times each number occurs and stores the result
  in a simple hydra tree.
```sh
$ ./docker.sh createSampleJob
creating sample job
{"id":"4d49b97a-d1e6-4616-9fec-9c338169b107","updated":"true"}
```
  - Run your job.  Notice the return from the create command in the previous step was a JSON string with an ID value.
  We will need that ID to run the job.  Replace $YOUR_JOB_ID with the ID returned in the previous step.
```sh
$ ./docker.sh runJob $YOUR_JOB_ID
```sh
  - Query the job (replace 192.168.59.103 with localhost if you are running on linux)
```sh
$ wget "http://192.168.59.103:6700/query/call?&job=$YOUR_JOB_ID&path=root%2F%2B%3A%2Bhits&ops=gather%3Dks%3Bsort%3D0&rops=gather%3Dks&format=json" -O /tmp/query.json
```
  - Results should be similar to:
```json
[["00000000000",296],["00000000001",299],["00000000002",282],["00000000003",284],["00000000004",303],["00000000005",302],["00000000006",311],["00000000007",300],["00000000008",317],["00000000009",306]]
```

## Next Steps

Now you have a functioning Hydra environment that you can use to help create your own Hydra jobs.

Learn more by reading the Hydra user documentation:

[The Hydra Documentation Page](http://oss-docs.addthiscode.net/hydra/latest/user-guide/index.html)
contains concepts, tutorials, guides, and the web api.

[The Hydra User Reference](http://oss-docs.clearspring.com/hydra/latest/user-reference/)
is built automatically from the source code and contains reference material
on hydra's configurable job components.

[Getting Started With Hydra](https://www.addthis.com/blog/2014/02/18/getting-started-with-hydra)
is a blog post that contains a nice self-contained introduction to hydra processing.
