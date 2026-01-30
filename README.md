# Flow Sentinel

Efficient data ingestion and monitoring with QuestDB integration, primarily leveraging Grafana for data visualization.

## Requirements

- Docker

## Usage

1. Build and start the services using Docker Compose:

   ```bash
   docker-compose up --build
   ```

2. Access the frontend application at `http://localhost:8501`.

3. Access the Grafana dashboard at `http://localhost:3000/d/flow-sentinel/flow-sentinel-dashboard`.

4. Add an event to the system by sending a request to the backend API:

   ```bash
   curl http://localhost:5000/event/<message>
   ```