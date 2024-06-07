# motor-asyncio-performance
Boost DB ops performane with AsycioMotorClient  &amp; Asyncio &amp; Uvloop 

[YouTube Video](https://youtu.be/8LBLXzAzWbM)

## Getting Started

1. Run Mongodb in Docker
```
docker-compose up -d
```

2. Set up Virtual Environment & Install Requirements

```
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt      
```


3. Populate product data as a source collection
```
python3 populate.py
```

Now, you should have 1.8M document in the source collection. What's next?

## Benchmark Write Operations

```
python3 db-migrate-slow.py
```


```
python3 db-migrate-fast.py
```

## Benchmark Read Operations (Existence Cheking)
With field existence vs assing default value


```
python3 db-bad-document-structure.py
```


```
python3 db-good-document-structure.py
```