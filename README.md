# Datenbanken-WS23

Aufgabe Datenbanken WS 23/24

Von Leopold von Wendt und Finn Mertens

## Deployment

In your terminal navigate to the deploy directory within this project and run
the following commands.

Create the network. You will only need to do this step once:
```terminal
docker network create backend
```

Then deploy the docker compose stack. This may take a while on the first run as
docker will first need to download all images:
```terminal
docker compose up -d
```

To shut the docker compose stack down after youre done use the following 
command:
```terminal
docker compose down -v
```
For your convenience there is also are also initialize.sh, run.sh and stop.sh
bash scripts, which will allow you to interact with the the stack without
navigating to the deploy/ directory. The initialize script only has to be ran
once before the first time you run the project. 

## Adminer

To inspect the underlying PostgreSQL database you can connect to the Adminer
interface using your webbrowser. To do this enter localhost:8080 in the address
bar after starting up the docker compose stack. For instructions on how to do
this see [Deployment](#Deployment).

To login to the control panel you will need to enter the following credentials
on the landing page:
1. Set the System to _PostgreSQL_
2. Set the server to _postgresdb:5432_
3. Set the username to _postgres_
4. Set the Password to _1234_
5. Set the Database to _dvdrental_ 

## Teamwork

| Task  | Leopold   | Finn  |
| ----- | :-------: | :---: |
| 2     | x         | x     |
| 3     | x         | x     |
| 4     | --------- | ----- |
| --- a | x         |       |
| --- b | x         |       |
| --- c | x         |       |
| --- d | x         |       |
| --- e | x         |       |
| --- f | x         |       |
| --- g |           | x     |
| --- h |           | x     |
| --- i |           | x     |
| 5     | --------- | ----- |
| --- a | x         |       |
| --- b |           | x     |
| 6     | --------- | ----- |
| --- a |           | x     |
| --- b |           | x     |

