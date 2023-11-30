# KeyValueStore

## Overview

This project implements a KeyValueStore with persistent storage using a Write-Ahead Log (WAL) and SSTable files. The KeyValueStore supports basic operations like setting a key-value pair, getting the value for a key, and deleting a key. The implementation includes recovery from the WAL on system restart.

## Getting Started

### Prerequisites

- Go installed on your machine

### Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/your-username/keyvaluestore.git
   cd keyvaluestore

2. **Run the server**
    ```bash
    go run main.go
The server will be available at http://localhost:8080.

### Usage

1. **Set a Key-Value Pair:**
To set a key-value pair, use the following curl command:
    ```bash
    curl -X POST -H "Content-Type: application/json" -d '{"key": "exampleKey", "value": "exampleValue"}' http://localhost:8080/set

2. **Get the Value for a Key:**
To retrieve the value for a key, use the following curl command:
    ```bash
    curl http://localhost:8080/get?key=exampleKey

3. **Delete a Key:**
To delete a key, use the following curl command:
    ```bash
    curl -X DELETE http://localhost:8080/del?key=exampleKey