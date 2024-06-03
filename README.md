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


3. Populate product data to source collection
```
python3 populate.py
```

4. Run Migrations & Compare

```
python3 db-migrate-slow.py
```


```
python3 db-migrate-fast.py
```