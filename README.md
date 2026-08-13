# Fingraph-
FinGraph is an advanced FinTech and Anti-Money Laundering (AML) project designed to detect complex fraud and money-laundering networks in real time.

## Week 1: Ingestion & Graph Schema

Week 1 focuses on generating realistic transaction data (including fraud syndicates, circular flows, and multi-hop intermediaries) and establishing the data pipeline infrastructure.

### Prerequisites
- Docker and Docker Compose
- Python 3.9+ 

### Setup Instructions

1. **Start the Infrastructure**
   Spin up Zookeeper, Kafka, and Neo4j using Docker:
   ```bash
   cd docker
   docker-compose up -d
   cd ..
   ```

2. **Python Environment Setup**
   Activate the virtual environment and install dependencies:
   ```powershell
   .\.venv\Scripts\Activate.ps1
   pip install -r simulator\requirements.txt
   ```

3. **Initialize Neo4j Schema**
   Apply the database constraints and indexes (Wait a few seconds for Neo4j to fully start):
   ```powershell
   Get-Content database\schema.cypher | docker exec -i neo4j cypher-shell -u neo4j -p password
   ```

4. **Run the Simulator (Producer)**
   Generate financial transactions (normal and suspicious) and push them to Kafka:
   ```powershell
   python simulator\main.py
   ```

5. **Verify the Stream (Consumer)**
   In a new terminal window (with the `.venv` activated), verify Kafka is receiving events:
   ```powershell
   python simulator\consumer_test.py
   ```

6. **Ingest to Neo4j**
   In another terminal window (with the `.venv` activated), stream the live Kafka data directly into the Neo4j graph:
   ```powershell
   python simulator\ingest_to_neo4j.py
   ```

### Validation
- Open the Neo4j Browser at []http://localhost:7474(http://localhost:7474) (Credentials: `neo4j` / `password`).
- Run `MATCH (n) RETURN n LIMIT 200` to visualize the live, interconnected financial graph.

