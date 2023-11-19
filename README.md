# Datenbanken-WS23

Aufgabe Datenbanken WS 23/24

Von Leopold von Wendt und Finn Mertens

## Deployment

In your terminal navigate to the deploy directory within this project and run
the following commands.

Create the network. You will only need to do this step once:
```terminal
docker create network backend
```

Then deploy the docker compose stack. This may take a while on the first run as
docker will first need to download all images:
```terminal
docker compose up -d
```

To shut the docker compose stack down after youre done use the following 
command:
```terminal
docker compose down
```

## Adminer

To inspect the underlying PostgreSQL database you can connect to the Adminer
interface using your webbrowser. To do this enter localhost:8080 in the address
bar after starting up the docker compose stack. For instructions on how to do
this see [Deployment](#Deployment).

## Teamwork

| Task  | Leopold   | Finn  |
| ----- | :-------: | :---: |
| 2     | x         |       |
| 3     |           |       |
| 4     |           |       |
| --- a |           |       |
| --- b |           |       |
| --- c |           |       |
| --- d |           |       |
| --- e |           |       |
| --- f |           |       |
| --- g |           |       |
| --- h |           |       |
| --- i |           |       |
| 5     |           |       |
| --- a |           |       |
| --- b |           |       |
| 6     |           |       |
| --- a |           |       |
| --- b |           |       |

