# Datenbanken-WS23

Aufgabe Datenbanken WS 23/24

Von Leopold von Wendt und Finn Mertens

## Deployment

In your terminal navigate to the deploy directory within this project and run
the following commands.

Create the network:
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